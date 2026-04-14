import fs from 'node:fs';
import path from 'node:path';
import process from 'node:process';
import { fileURLToPath } from 'node:url';
import XLSX from 'xlsx';
import { createClient } from '@supabase/supabase-js';

const scriptDir = path.dirname(fileURLToPath(import.meta.url));
const frontendDir = path.resolve(scriptDir, '..');
const projectDir = path.resolve(frontendDir, '..');
const envPath = path.join(frontendDir, '.env');
const historyPath = path.join(projectDir, 'data', 'history.xlsx');
const CHUNK_SIZE = 100;
const MAX_RETRIES = 5;
const RETRY_DELAY_MS = 2000;
const INTER_CHUNK_DELAY_MS = 150;
const RESET_IMPORT = process.argv.includes('--reset');

function readEnvFile(filePath) {
  if (!fs.existsSync(filePath)) return {};

  return fs
    .readFileSync(filePath, 'utf8')
    .split(/\r?\n/)
    .reduce((env, rawLine) => {
      const line = rawLine.trim();
      if (!line || line.startsWith('#')) return env;

      const separatorIndex = line.indexOf('=');
      if (separatorIndex === -1) return env;

      const key = line.slice(0, separatorIndex).trim();
      let value = line.slice(separatorIndex + 1).trim();

      if (
        (value.startsWith('"') && value.endsWith('"')) ||
        (value.startsWith("'") && value.endsWith("'"))
      ) {
        value = value.slice(1, -1);
      }

      env[key] = value;
      return env;
    }, {});
}

function getSetting(env, ...keys) {
  for (const key of keys) {
    const value = process.env[key] ?? env[key];
    if (value) return value;
  }

  return '';
}

function normalizeIdentifier(value) {
  if (value === undefined || value === null || value === '') return null;

  let normalized = value;
  if (typeof normalized === 'number' && Number.isInteger(normalized)) {
    normalized = String(normalized);
  } else {
    normalized = String(normalized).trim().toUpperCase();
  }

  if (normalized.endsWith('.0') && /^\d+\.0$/.test(normalized)) {
    normalized = normalized.slice(0, -2);
  }

  normalized = normalized.replace(/\s+/g, '');
  return normalized || null;
}

function excelSerialToDate(value) {
  const utcDays = Math.floor(value - 25569);
  const utcValue = utcDays * 86400;
  const fractionalDay = value - Math.floor(value) + 0.0000001;
  const totalSeconds = Math.floor(86400 * fractionalDay);
  const seconds = totalSeconds % 60;
  const hours = Math.floor(totalSeconds / 3600);
  const minutes = Math.floor(totalSeconds / 60) % 60;
  const date = new Date(utcValue * 1000);
  date.setUTCHours(hours, minutes, seconds, 0);
  return date;
}

function toIsoDateTime(value) {
  if (value === undefined || value === null || value === '') return null;
  if (value instanceof Date) return Number.isNaN(value.getTime()) ? null : value.toISOString();
  if (typeof value === 'number') {
    const date = excelSerialToDate(value);
    return Number.isNaN(date.getTime()) ? null : date.toISOString();
  }

  const parsed = new Date(String(value));
  return Number.isNaN(parsed.getTime()) ? null : parsed.toISOString();
}

function makeJsonSafe(value) {
  if (value instanceof Date) return Number.isNaN(value.getTime()) ? null : value.toISOString();
  if (Array.isArray(value)) return value.map((item) => makeJsonSafe(item));
  if (value && typeof value === 'object') {
    return Object.fromEntries(
      Object.entries(value).map(([key, item]) => [key, makeJsonSafe(item)]),
    );
  }
  if (value === undefined) return null;
  if (typeof value === 'number' && !Number.isFinite(value)) return null;
  return value ?? null;
}

function chunkValues(values, size) {
  const chunks = [];
  for (let index = 0; index < values.length; index += size) {
    chunks.push(values.slice(index, index + size));
  }
  return chunks;
}

function toErrorMessage(error) {
  return error?.message || error?.error_description || error?.details || String(error);
}

function sleep(ms) {
  return new Promise((resolve) => {
    setTimeout(resolve, ms);
  });
}

async function insertChunkWithRetry(supabase, chunk, chunkIndex, totalChunks) {
  for (let attempt = 1; attempt <= MAX_RETRIES; attempt += 1) {
    const { error } = await supabase.from('history_records').insert(chunk);
    if (!error) return;

    const message = toErrorMessage(error);
    const isLastAttempt = attempt === MAX_RETRIES;
    if (isLastAttempt) {
      throw new Error(`Chunk ${chunkIndex}/${totalChunks} failed after ${MAX_RETRIES} attempts: ${message}`);
    }

    console.warn(
      `Chunk ${chunkIndex}/${totalChunks} failed on attempt ${attempt}/${MAX_RETRIES}: ${message}. Retrying...`,
    );
    await sleep(RETRY_DELAY_MS * attempt);
  }
}

async function getExistingImportState(supabase) {
  const { count, error: countError } = await supabase
    .from('history_records')
    .select('*', { count: 'exact', head: true });

  if (countError) {
    const message = toErrorMessage(countError);
    if (message.toLowerCase().includes('schema cache') || message.toLowerCase().includes('could not find the table')) {
      throw new Error('history_records does not exist yet. Run supabase/schema.sql in the Supabase SQL Editor first.');
    }
    throw new Error(`Failed to inspect history_records: ${message}`);
  }

  if (!count) {
    return { count: 0, maxImportRowNumber: null };
  }

  const { data, error } = await supabase
    .from('history_records')
    .select('import_row_number')
    .order('import_row_number', { ascending: false })
    .limit(1)
    .maybeSingle();

  if (error) {
    throw new Error(`Failed to inspect history_records progress: ${toErrorMessage(error)}`);
  }

  return {
    count,
    maxImportRowNumber: data?.import_row_number ?? null,
  };
}

async function run() {
  const env = readEnvFile(envPath);
  const supabaseUrl = getSetting(env, 'SUPABASE_URL', 'VITE_SUPABASE_URL');
  const supabaseKey = getSetting(env, 'SUPABASE_SERVICE_ROLE_KEY', 'VITE_SUPABASE_ANON_KEY');

  if (!supabaseUrl) {
    throw new Error(`Missing SUPABASE_URL or VITE_SUPABASE_URL in ${envPath}`);
  }

  if (!supabaseKey) {
    throw new Error(`Missing SUPABASE_SERVICE_ROLE_KEY or VITE_SUPABASE_ANON_KEY in ${envPath}`);
  }

  if (!fs.existsSync(historyPath)) {
    throw new Error(`Could not find history workbook at ${historyPath}`);
  }

  const supabase = createClient(supabaseUrl, supabaseKey, {
    auth: {
      autoRefreshToken: false,
      persistSession: false,
    },
  });

  console.log(`Reading history workbook from ${historyPath} ...`);
  const workbook = XLSX.readFile(historyPath, { cellDates: true, raw: true });
  const firstSheet = workbook.Sheets[workbook.SheetNames[0]];
  const rows = XLSX.utils.sheet_to_json(firstSheet, {
    raw: true,
    defval: null,
    range: 1,
  });

  console.log(`Found ${rows.length} row(s) in history.xlsx.`);

  const records = rows.map((row, index) => ({
    account_number: row.ACCOUNT_NUMBER !== null ? String(row.ACCOUNT_NUMBER) : null,
    customer_name1: row.CUSTOMER_NAME1 !== null ? String(row.CUSTOMER_NAME1) : null,
    ec_number: row.EC_NUMBER !== null ? String(row.EC_NUMBER) : null,
    customer_no: row.CUSTOMER_NO !== null ? String(row.CUSTOMER_NO) : null,
    amount_financed: row.AMOUNT_FINANCED !== null && row.AMOUNT_FINANCED !== '' ? Number(row.AMOUNT_FINANCED) : null,
    book_date: toIsoDateTime(row.BOOK_DATE),
    import_row_number: index + 2,
    normalized_ec_number: normalizeIdentifier(row.EC_NUMBER),
    normalized_customer_no: normalizeIdentifier(row.CUSTOMER_NO),
    row_data: makeJsonSafe(row),
  }));

  const existingState = await getExistingImportState(supabase);
  let startIndex = 0;

  if (RESET_IMPORT) {
    const { error: deleteError } = await supabase
      .from('history_records')
      .delete()
      .not('id', 'is', null);

    if (deleteError) {
      throw new Error(`Failed to clear history_records: ${toErrorMessage(deleteError)}`);
    }
    console.log('Cleared existing history_records before reimporting.');
  } else if (existingState.count > 0) {
    startIndex = Math.max(existingState.count, (existingState.maxImportRowNumber ?? 1) - 1);
    console.log(`Detected ${existingState.count} existing row(s) in history_records.`);
    if (startIndex >= records.length) {
      console.log('history_records already contains the full workbook. Nothing to import.');
      return;
    }
    console.log(`Resuming import from row ${startIndex + 1} of ${records.length}.`);
  }

  let inserted = startIndex;
  const remainingRecords = records.slice(startIndex);
  const chunks = chunkValues(remainingRecords, CHUNK_SIZE);
  for (let index = 0; index < chunks.length; index += 1) {
    const chunk = chunks[index];
    await insertChunkWithRetry(supabase, chunk, index + 1, chunks.length);
    inserted += chunk.length;
    console.log(`Inserted chunk ${index + 1}/${chunks.length} (${inserted}/${records.length})`);
    if (INTER_CHUNK_DELAY_MS > 0) {
      await sleep(INTER_CHUNK_DELAY_MS);
    }
  }

  const { count, error: countError } = await supabase
    .from('history_records')
    .select('*', { count: 'exact', head: true });

  if (countError) {
    throw new Error(`Import finished, but count check failed: ${toErrorMessage(countError)}`);
  }

  console.log(`History import complete. Supabase now has ${count ?? inserted} row(s) in history_records.`);
}

run().catch((error) => {
  console.error(toErrorMessage(error));
  process.exitCode = 1;
});
