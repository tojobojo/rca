import smtplib
from typing import List, Dict, Any

from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail

from config.settings import Config
from core.exceptions import EmailNotificationError
from utils.logging_config import setup_logging

logger = setup_logging("email_sender")


class EmailSender:
    """Send RCA email notifications via SMTP or SendGrid"""

    def __init__(self):
        self.provider = Config.rca.EMAIL_PROVIDER.lower()
        self.from_email = Config.rca.EMAIL_FROM

        if self.provider == "sendgrid":
            self.sg = SendGridAPIClient(Config.rca.SENDGRID_API_KEY)

    def send_rca_email(
        self,
        to_emails: List[str],
        pipeline_name: str,
        severity: str,
        summary: str,
        raw_data: Dict[str, Any],
        chatbot_link: str = None
    ):
        logger.info(
            f"Sending RCA email via {self.provider} "
            f"for {pipeline_name} to {len(to_emails)} recipients"
        )

        if not Config.rca.SEND_EMAILS:
            logger.info("Email sending disabled by configuration")
            return

        subject = self._format_subject(pipeline_name, severity)
        html_body = self._format_email_body(
            pipeline_name, severity, summary, raw_data, chatbot_link
        )

        try:
            if self.provider == "smtp":
                self._send_via_smtp(to_emails, subject, html_body)
            elif self.provider == "sendgrid":
                self._send_via_sendgrid(to_emails, subject, html_body)
            else:
                raise ValueError(f"Unsupported EMAIL_PROVIDER: {self.provider}")

            logger.info("RCA email sent successfully")

        except Exception as e:
            logger.error("Failed to send RCA email", exc_info=True)
            raise EmailNotificationError(str(e))

    # ─────────────────────────────────────
    # SMTP
    # ─────────────────────────────────────
    def _send_via_smtp(self, to_emails: List[str], subject: str, html_body: str):
        msg = MIMEMultipart("alternative")
        msg["From"] = self.from_email
        msg["To"] = ", ".join(to_emails)
        msg["Subject"] = subject

        msg.attach(MIMEText(html_body, "html"))

        with smtplib.SMTP(Config.rca.SMTP_HOST, Config.rca.SMTP_PORT) as server:
            server.starttls()
            server.login(
                Config.rca.SMTP_USER,
                Config.rca.SMTP_PASSWORD
            )
            server.send_message(msg)

    # ─────────────────────────────────────
    # SendGrid
    # ─────────────────────────────────────
    def _send_via_sendgrid(self, to_emails: List[str], subject: str, html_body: str):
        message = Mail(
            from_email=self.from_email,
            to_emails=to_emails,
            subject=subject,
            html_content=html_body
        )
        response = self.sg.send(message)

        if response.status_code >= 400:
            raise EmailNotificationError(
                f"SendGrid error {response.status_code}: {response.body}"
            )

    # ─────────────────────────────────────
    # Formatting
    # ─────────────────────────────────────
    def _format_subject(self, pipeline_name: str, severity: str) -> str:
        emoji_map = {
            "critical": "🔴",
            "warning": "⚠️",
            "normal": "✅"
        }
        emoji = emoji_map.get(severity, "📊")
        return f"{emoji} [{severity.upper()}] Pipeline RCA - {pipeline_name}"

    def _format_email_body(
        self,
        pipeline_name: str,
        severity: str,
        summary: str,
        raw_data: Dict[str, Any],
        chatbot_link: str = None
    ) -> str:
        """Format HTML email body"""

        metrics = raw_data['metrics_summary']

        # Color scheme based on severity
        color_map = {
            'critical': '#dc3545',
            'warning': '#ffc107',
            'normal': '#28a745'
        }
        severity_color = color_map.get(severity, '#6c757d')

        html = f"""
<!DOCTYPE html>
<html>
<head>
    <style>
        body {{
            font-family: Arial, sans-serif;
            line-height: 1.6;
            color: #333;
            max-width: 800px;
            margin: 0 auto;
            padding: 20px;
        }}
        .header {{
            background-color: {severity_color};
            color: white;
            padding: 20px;
            border-radius: 5px;
            margin-bottom: 20px;
        }}
        .metrics-grid {{
            display: grid;
            grid-template-columns: repeat(2, 1fr);
            gap: 15px;
            margin: 20px 0;
        }}
        .metric-card {{
            background-color: #f8f9fa;
            padding: 15px;
            border-radius: 5px;
            border-left: 4px solid {severity_color};
        }}
        .metric-label {{
            font-size: 0.85em;
            color: #666;
            margin-bottom: 5px;
        }}
        .metric-value {{
            font-size: 1.5em;
            font-weight: bold;
            color: #333;
        }}
        .summary-section {{
            background-color: #fff;
            border: 1px solid #dee2e6;
            border-radius: 5px;
            padding: 20px;
            margin: 20px 0;
        }}
        .summary-section h3 {{
            margin-top: 0;
            color: #333;
        }}
        .action-button {{
            display: inline-block;
            padding: 12px 24px;
            background-color: #007bff;
            color: white !important;
            text-decoration: none;
            border-radius: 5px;
            margin: 10px 5px;
        }}
        .footer {{
            margin-top: 30px;
            padding-top: 20px;
            border-top: 1px solid #dee2e6;
            font-size: 0.9em;
            color: #666;
        }}
    </style>
</head>
<body>
    <div class="header">
        <h1>Pipeline RCA Summary</h1>
        <p><strong>Pipeline:</strong> {pipeline_name}</p>
        <p><strong>Run ID:</strong> {raw_data['run_id']}</p>
        <p><strong>Timestamp:</strong> {raw_data['timestamp']}</p>
        <p><strong>Severity:</strong> {severity.upper()}</p>
    </div>
    
    <div class="metrics-grid">
        <div class="metric-card">
            <div class="metric-label">Total Input Records</div>
            <div class="metric-value">{metrics['total_input_records']:,}</div>
        </div>
        <div class="metric-card">
            <div class="metric-label">Total Dropped Records</div>
            <div class="metric-value">{metrics['total_dropped_records']:,}</div>
        </div>
        <div class="metric-card">
            <div class="metric-label">Current Drop Rate</div>
            <div class="metric-value">{metrics['current_drop_pct']}%</div>
        </div>
        <div class="metric-card">
            <div class="metric-label">Change vs Previous</div>
            <div class="metric-value">{metrics.get('drop_delta', 0):+.1f}%</div>
        </div>
    </div>
    
    <div class="summary-section">
        <h3>Root Cause Analysis</h3>
        <p style="white-space: pre-wrap;">{summary}</p>
    </div>
    
    <div style="text-align: center; margin: 30px 0;">
        <p><strong>Need more details?</strong></p>
        {f'<a href="{chatbot_link}" class="action-button">🤖 Chat with RCA Bot</a>' if chatbot_link else ''}
        <a href="#" class="action-button">📊 View Dashboard</a>
    </div>
    
    <div class="footer">
        <p>This is an automated RCA notification from the Pipeline Monitoring System.</p>
        <p>If you have questions, please reach out to the Data Engineering team.</p>
    </div>
</body>
</html>
"""
        return html
