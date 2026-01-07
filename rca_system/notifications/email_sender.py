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
        """Format HTML email body with enhanced visuals"""

        metrics = raw_data['metrics_summary']
        step_table = self._format_step_table(raw_data['step_analysis'])
        
        # Calculate trends
        drop_delta = metrics.get('drop_delta', 0)
        trend_arrow, trend_class = self._calculate_trend(drop_delta)
        
        # Colors
        color_map = {
            'critical': '#dc3545',
            'warning': '#ffc107',
            'normal': '#28a745'
        }
        severity_color = color_map.get(severity, '#6c757d')
        badge_bg = f"{severity_color}20" # 20% opacity
        badge_color = severity_color

        html = f"""
<!DOCTYPE html>
<html>
<head>
    <style>
        body {{
            font-family: 'Segoe UI', Roboto, Helvetica, Arial, sans-serif;
            line-height: 1.6;
            color: #333;
            max-width: 800px;
            margin: 0 auto;
            padding: 20px;
            background-color: #f4f6f9;
        }}
        .container {{
            background-color: #ffffff;
            border-radius: 8px;
            box-shadow: 0 2px 10px rgba(0,0,0,0.05);
            overflow: hidden;
        }}
        .header {{
            background-color: {severity_color};
            color: white;
            padding: 25px;
            text-align: center;
        }}
        .header h1 {{ margin: 0; font-size: 24px; }}
        .severity-badge {{
            display: inline-block;
            padding: 5px 12px;
            background-color: rgba(255,255,255,0.2);
            border-radius: 20px;
            font-size: 0.9em;
            margin-top: 10px;
            font-weight: bold;
        }}
        .content {{ padding: 30px; }}
        
        .section-title {{
            font-size: 18px;
            font-weight: 600;
            color: #2c3e50;
            margin-top: 25px;
            margin-bottom: 15px;
            padding-bottom: 8px;
            border-bottom: 2px solid #f0f0f0;
        }}
        
        .metrics-grid {{
            display: grid;
            grid-template-columns: repeat(2, 1fr);
            gap: 15px;
            margin-bottom: 25px;
        }}
        .metric-card {{
            background-color: #f8f9fa;
            padding: 15px;
            border-radius: 6px;
            border-left: 4px solid {severity_color};
        }}
        .metric-label {{ font-size: 0.85em; color: #666; margin-bottom: 5px; }}
        .metric-value {{ font-size: 1.6em; font-weight: bold; color: #2c3e50; }}
        .trend-indicator {{ font-size: 0.6em; padding: 2px 6px; border-radius: 4px; vertical-align: middle; margin-left: 8px; }}
        .trend-up {{ background-color: #ffebee; color: #c62828; }}
        .trend-down {{ background-color: #e8f5e9; color: #2e7d32; }}
        .trend-neutral {{ background-color: #f5f5f5; color: #616161; }}
        
        .findings-box {{
            background-color: #fff8e1;
            border-left: 4px solid #ffc107;
            padding: 15px;
            border-radius: 4px;
            margin-bottom: 25px;
        }}
        
        .step-table {{
            width: 100%;
            border-collapse: collapse;
            font-size: 0.9em;
            margin-bottom: 25px;
        }}
        .step-table th {{ text-align: left; padding: 12px; background-color: #f8f9fa; border-bottom: 2px solid #dee2e6; color: #495057; }}
        .step-table td {{ padding: 12px; border-bottom: 1px solid #dee2e6; }}
        .step-row-highlight {{ background-color: #fff5f5; }}
        
        .action-buttons {{ margin-top: 30px; text-align: center; }}
        .btn {{
            display: inline-block;
            padding: 12px 24px;
            color: white;
            text-decoration: none;
            border-radius: 6px;
            font-weight: 500;
            margin: 0 5px;
            transition: opacity 0.2s;
        }}
        .btn-primary {{ background-color: #007bff; }}
        .btn-secondary {{ background-color: #6c757d; }}
        
        .footer {{
            text-align: center;
            font-size: 0.85em;
            color: #999;
            margin-top: 30px;
            padding-top: 20px;
            border-top: 1px solid #eee;
        }}
        
        @media (max-width: 600px) {{
            .metrics-grid {{ grid-template-columns: 1fr; }}
            .btn {{ display: block; margin: 10px 0; }}
            .container {{ border-radius: 0; }}
        }}
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>Pipeline RCA Report</h1>
            <div class="severity-badge">{severity.upper()}</div>
        </div>
        
        <div class="content">
            <p style="margin-bottom: 5px;"><strong>Pipeline:</strong> {pipeline_name}</p>
            <p style="margin-top: 0; color: #666; font-size: 0.9em;">
                <strong>Run ID:</strong> {raw_data['run_id']} • 
                <strong>Time:</strong> {raw_data['timestamp']}
            </p>
            
            <div class="findings-box">
                <h3 style="margin-top: 0; margin-bottom: 10px; color: #856404;">🎯 Executive Summary</h3>
                <div style="white-space: pre-wrap!important;">{summary}</div>
            </div>
            
            <div class="metrics-grid">
                <div class="metric-card">
                    <div class="metric-label">Current Drop Rate</div>
                    <div class="metric-value">
                        {metrics['current_drop_pct']}% 
                        <span class="trend-indicator {trend_class}" title="Compared to previous run">{trend_arrow} {abs(drop_delta):.2f}% vs prev</span>
                    </div>
                </div>
                <div class="metric-card">
                    <div class="metric-label">Total Dropped Records</div>
                    <div class="metric-value">{metrics['total_dropped_records']:,}</div>
                </div>
                <div class="metric-card">
                    <div class="metric-label">Input Records</div>
                    <div class="metric-value">{metrics['total_input_records']:,}</div>
                </div>
                <div class="metric-card">
                    <div class="metric-label">7-Day Baseline</div>
                    <div class="metric-value">{metrics['baseline_7day_avg']}%</div>
                </div>
            </div>
            
            <div class="section-title">📍 Step-Level Breakdown</div>
            {step_table}

            <div class="section-title">👨‍💻 Code Change Analysis</div>
            {self._format_code_changes(raw_data.get('correlations', []))}
            
            <div class="section-title">💼 Business Impact</div>
            <p>
                The current drop rate of <strong>{metrics['current_drop_pct']}%</strong> is 
                {abs(metrics['current_drop_pct'] - metrics['baseline_30day_avg']):.2f}% 
                {'above' if metrics['current_drop_pct'] > metrics['baseline_30day_avg'] else 'below'} 
                the <strong>30-day baseline of {metrics['baseline_30day_avg']}%</strong>.
            </p>
            <p>
                <strong>{metrics['total_dropped_records']:,} records</strong> were excluded this run, 
                potentially impacting downstream reporting accuracy.
            </p>
            
            <div class="action-buttons">
                {f'<a href="{chatbot_link}" class="btn btn-primary">🤖 Investigate in Chatbot</a>' if chatbot_link else ''}
                <a href="#" class="btn btn-secondary">📊 View Dashboard</a>
                <a href="#" class="btn btn-secondary">📖 Runbook</a>
            </div>
            
            <div class="footer">
                <p>Generated automatically by RCA System • <a href="#">Feedback</a> • <a href="#">Documentation</a></p>
            </div>
        </div>
    </div>
</body>
</html>
"""
        return html

    def _format_step_table(self, step_analysis: List[Dict]) -> str:
        """Format step analysis list into HTML table"""
        if not step_analysis:
            return "<p>No step data available</p>"
            
        rows = []
        for step in step_analysis[:5]:  # Top 5 steps
            drop_pct = step.get('drop_percentage', 0)
            is_high = drop_pct > 10.0  # Simple threshold for highlighting
            row_class = 'class="step-row-highlight"' if is_high else ''
            
            rows.append(f"""
            <tr {row_class}>
                <td>{step['step_name']}</td>
                <td>{step['dropped_records']:,}</td>
                <td><strong>{drop_pct}%</strong></td>
            </tr>
            """)
            
        return f"""
        <table class="step-table">
            <thead>
                <tr>
                    <th>Step Name</th>
                    <th>Dropped Records</th>
                    <th>Drop Rate</th>
                </tr>
            </thead>
            <tbody>
                {''.join(rows)}
            </tbody>
        </table>
        """

    def _calculate_trend(self, delta: float):
        """Calculate trend arrow and class based on delta"""
        if delta > 0.1:
            return "↑", "trend-up"
        elif delta < -0.1:
            return "↓", "trend-down"
        else:
            return "→", "trend-neutral"

    def _format_code_changes(self, correlations: List[Dict]) -> str:
        """Format code changes into HTML code blocks"""
        if not correlations:
            return "<p>No code changes correlated with this run.</p>"
            
        html = ""
        for corr in correlations:
            html += f"""
            <div style="background-color: #f8f9fa; border: 1px solid #dee2e6; border-radius: 4px; padding: 15px; margin-bottom: 15px;">
                <div style="font-weight: bold; color: #333; margin-bottom: 5px;">
                    {corr['rule']}
                    <span style="font-size: 0.8em; color: #666; font-weight: normal;">({corr.get('author', 'Unknown Author')})</span>
                </div>
                <div style="font-family: monospace; background-color: #2b2b2b; color: #f8f8f2; padding: 10px; border-radius: 4px; overflow-x: auto;">
                    {corr.get('change_summary', 'No details available')}
                </div>
                <div style="font-size: 0.85em; color: #666; margin-top: 5px;">
                    Impact: <strong>{corr['change_delta']:+.2f}%</strong> drop rate • Commit: <code>{corr.get('commit_hash', 'N/A')[:8]}</code>
                </div>
            </div>
            """
        return html
