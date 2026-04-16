const corsHeaders = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'authorization, x-client-info, apikey, content-type',
  'Access-Control-Allow-Methods': 'POST, OPTIONS',
};

type SendCsvReportPayload = {
  fileName?: string;
  sourceFileName?: string;
  acceptedCount?: number;
  rejectedCount?: number;
  generatedAt?: string;
  pdfBase64?: string;
};

Deno.serve(async request => {
  if (request.method === 'OPTIONS') {
    return new Response('ok', { headers: corsHeaders });
  }

  if (request.method !== 'POST') {
    return new Response(JSON.stringify({ error: 'Method not allowed.' }), {
      status: 405,
      headers: { ...corsHeaders, 'Content-Type': 'application/json' },
    });
  }

  let payload: SendCsvReportPayload;
  try {
    payload = await request.json();
  } catch {
    return new Response(JSON.stringify({ error: 'Invalid JSON payload.' }), {
      status: 400,
      headers: { ...corsHeaders, 'Content-Type': 'application/json' },
    });
  }

  const fileName = String(payload.fileName ?? '').trim();
  const sourceFileName = String(payload.sourceFileName ?? '').trim();
  const pdfBase64 = String(payload.pdfBase64 ?? '').trim();
  const acceptedCount = Number(payload.acceptedCount ?? 0);
  const rejectedCount = Number(payload.rejectedCount ?? 0);
  const generatedAt = String(payload.generatedAt ?? '').trim();
  const recipient = String(Deno.env.get('REPORT_TO_EMAIL') ?? 'aiqkanyoka@gmail.com').trim();

  if (!fileName || !sourceFileName || !pdfBase64 || !recipient) {
    return new Response(JSON.stringify({ error: 'Missing required report details.' }), {
      status: 400,
      headers: { ...corsHeaders, 'Content-Type': 'application/json' },
    });
  }

  const resendApiKey = Deno.env.get('RESEND_API_KEY');
  const reportFromEmail = Deno.env.get('REPORT_FROM_EMAIL');

  if (!resendApiKey || !reportFromEmail) {
    return new Response(JSON.stringify({
      error: 'Email delivery is not configured. Set RESEND_API_KEY and REPORT_FROM_EMAIL in Supabase secrets.',
    }), {
      status: 500,
      headers: { ...corsHeaders, 'Content-Type': 'application/json' },
    });
  }

  const generatedLabel = generatedAt
    ? new Date(generatedAt).toLocaleString('en-US', {
        year: 'numeric',
        month: 'short',
        day: '2-digit',
        hour: '2-digit',
        minute: '2-digit',
      })
    : 'Unknown';

  const resendResponse = await fetch('https://api.resend.com/emails', {
    method: 'POST',
    headers: {
      Authorization: `Bearer ${resendApiKey}`,
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({
      from: reportFromEmail,
      to: [recipient],
      subject: `CSV Response Report - ${sourceFileName}`,
      html: `
        <div style="font-family: Arial, sans-serif; line-height: 1.5; color: #0f172a;">
          <h2 style="margin-bottom: 12px;">CSV Response Report</h2>
          <p style="margin: 0 0 8px;"><strong>Source file:</strong> ${sourceFileName}</p>
          <p style="margin: 0 0 8px;"><strong>Generated:</strong> ${generatedLabel}</p>
          <p style="margin: 0 0 8px;"><strong>Accepted records:</strong> ${acceptedCount}</p>
          <p style="margin: 0 0 16px;"><strong>Rejected records:</strong> ${rejectedCount}</p>
          <p style="margin: 0;">The PDF report is attached to this email.</p>
        </div>
      `,
      attachments: [
        {
          filename: fileName,
          content: pdfBase64,
        },
      ],
    }),
  });

  if (!resendResponse.ok) {
    const errorText = await resendResponse.text();
    return new Response(JSON.stringify({
      error: errorText || 'Failed to send the email via Resend.',
    }), {
      status: 502,
      headers: { ...corsHeaders, 'Content-Type': 'application/json' },
    });
  }

  return new Response(JSON.stringify({
    message: `Report emailed to ${recipient}.`,
    recipient,
  }), {
    status: 200,
    headers: { ...corsHeaders, 'Content-Type': 'application/json' },
  });
});
