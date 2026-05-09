"""
Bundle the day's analytical outputs into a single Analyzed Report PDF.

Inputs (produced by the three sibling tasks):
  - conversion_rates_<date>.csv
  - conversion_rates_<date>.png
  - top5_products_<date>.txt
  - user_segments_<date>.csv

Output:
  - analyzed_report_<date>.pdf
"""

from __future__ import annotations

import os

import pandas as pd
from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import cm
from reportlab.platypus import (
    Image,
    PageBreak,
    Paragraph,
    SimpleDocTemplate,
    Spacer,
    Table,
    TableStyle,
)


def _df_to_table(df: pd.DataFrame, max_rows: int | None = None) -> Table:
    if max_rows is not None and len(df) > max_rows:
        df = df.head(max_rows)
    data = [list(df.columns)] + df.astype(str).values.tolist()
    table = Table(data, repeatRows=1)
    table.setStyle(TableStyle([
        ("BACKGROUND",  (0, 0), (-1, 0),  colors.HexColor("#1f3a93")),
        ("TEXTCOLOR",   (0, 0), (-1, 0),  colors.white),
        ("FONTNAME",    (0, 0), (-1, 0),  "Helvetica-Bold"),
        ("FONTSIZE",    (0, 0), (-1, -1), 9),
        ("ALIGN",       (0, 0), (-1, -1), "LEFT"),
        ("VALIGN",      (0, 0), (-1, -1), "MIDDLE"),
        ("GRID",        (0, 0), (-1, -1), 0.25, colors.grey),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1),
            [colors.whitesmoke, colors.HexColor("#eef2f7")]),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
        ("TOPPADDING",    (0, 0), (-1, -1), 4),
    ]))
    return table


def run(out_path: str, logical_date: str) -> None:
    csv_conv = os.path.join(out_path, f"conversion_rates_{logical_date}.csv")
    png_conv = os.path.join(out_path, f"conversion_rates_{logical_date}.png")
    txt_top5 = os.path.join(out_path, f"top5_products_{logical_date}.txt")
    csv_users = os.path.join(out_path, f"user_segments_{logical_date}.csv")
    pdf_file = os.path.join(out_path, f"analyzed_report_{logical_date}.pdf")

    for required in (csv_conv, png_conv, txt_top5, csv_users):
        if not os.path.exists(required):
            raise FileNotFoundError(
                f"Expected upstream output not found: {required}. "
                f"Make sure the other report tasks ran first."
            )

    styles = getSampleStyleSheet()
    title_style = ParagraphStyle(
        "Title",
        parent=styles["Heading1"],
        fontSize=20,
        textColor=colors.HexColor("#1f3a93"),
        spaceAfter=12,
    )
    h2 = ParagraphStyle(
        "H2",
        parent=styles["Heading2"],
        fontSize=14,
        textColor=colors.HexColor("#1f3a93"),
        spaceBefore=10,
        spaceAfter=6,
    )
    body = styles["BodyText"]
    mono = ParagraphStyle(
        "Mono", parent=body, fontName="Courier", fontSize=9, leading=11
    )

    doc = SimpleDocTemplate(
        pdf_file,
        pagesize=A4,
        leftMargin=1.8 * cm,
        rightMargin=1.8 * cm,
        topMargin=1.6 * cm,
        bottomMargin=1.6 * cm,
        title=f"E-Commerce Analyzed Report {logical_date}",
        author="ABDA Mini Project",
    )

    story = []

    # Cover 
    story.append(Paragraph("E-Commerce Clickstream &amp; Inventory Watch", title_style))
    story.append(Paragraph(f"Analyzed Report &mdash; {logical_date}", h2))
    story.append(Spacer(1, 6))
    story.append(Paragraph(
        "End-to-end Lambda Architecture pipeline. Real-time clickstream events were "
        "ingested via Kafka, processed by Spark Structured Streaming (10-minute "
        "sliding window with a 2-minute watermark on event time), archived to "
        "Parquet partitioned by date, and aggregated by an Airflow DAG to produce "
        "the analytics below.",
        body,
    ))
    story.append(Spacer(1, 12))

    # Section 1: Conversion rates 
    story.append(Paragraph("1. Conversion Rate by Product Category", h2))
    story.append(Paragraph(
        "Conversion rate is computed as <i>purchases / views</i> per category "
        "across the full day's traffic. The chart is the headline analytical "
        "deliverable for this scenario.",
        body,
    ))
    story.append(Spacer(1, 6))
    story.append(Image(png_conv, width=16 * cm, height=9 * cm))
    story.append(Spacer(1, 8))

    df_conv = pd.read_csv(csv_conv)
    if "conversion_rate" in df_conv.columns:
        df_conv["conversion_rate"] = (df_conv["conversion_rate"] * 100).round(2).astype(str) + "%"
    story.append(_df_to_table(df_conv))

    story.append(PageBreak())

    #  Section 2: Top-5 products
    story.append(Paragraph("2. Top 5 Most-Viewed Products", h2))
    story.append(Paragraph(
        "These are the five products that received the highest number of "
        "<i>view</i> events during the day &mdash; the merchandising team would "
        "use this to surface stock-level decisions.",
        body,
    ))
    story.append(Spacer(1, 6))
    with open(txt_top5, "r", encoding="utf-8") as fh:
        top5_text = fh.read()
    for line in top5_text.splitlines():
        # Use non-breaking spaces so reportlab keeps the column alignment.
        safe = line.replace(" ", "&nbsp;").replace("<", "&lt;").replace(">", "&gt;")
        story.append(Paragraph(safe, mono))

    story.append(PageBreak())

    #  Section 3: User segmentation
    story.append(Paragraph("3. User Segmentation &mdash; Window Shoppers vs Buyers", h2))
    df_users = pd.read_csv(csv_users)
    total = len(df_users)
    buyers = int((df_users["segment"] == "BUYER").sum())
    shoppers = total - buyers
    buyer_pct = (buyers / total * 100) if total else 0.0
    story.append(Paragraph(
        f"Out of <b>{total}</b> active users today, <b>{buyers}</b> "
        f"({buyer_pct:.1f}%) made at least one purchase (BUYER) and "
        f"<b>{shoppers}</b> ({100 - buyer_pct:.1f}%) only browsed "
        f"(WINDOW_SHOPPER).",
        body,
    ))
    story.append(Spacer(1, 8))
    story.append(Paragraph("Top 25 most-active users:", body))
    story.append(Spacer(1, 4))

    df_users_top = df_users.sort_values(
        ["purchases", "views"], ascending=[False, False]
    ).head(25)
    story.append(_df_to_table(df_users_top))

    # Build 
    doc.build(story)
    print(f"[build_pdf_report] Wrote -> {pdf_file}")
