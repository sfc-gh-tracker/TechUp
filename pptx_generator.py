import os
from pptx import Presentation
from pptx.util import Inches, Pt

ROOT = os.path.dirname(os.path.abspath(__file__))

slides_spec = [
    {
        "folder": "Adaptive Right-Sizing",
        "title": "Adaptive Warehouse Right-Sizing",
        "bullets": [
            "Computes hour-by-hour sizing policy from metering (credits_used)",
            "Dynamic Table: RIGHT_SIZING_POLICY_DT",
            "Executor: APPLY_RIGHT_SIZING() + scheduled task",
            "Seed script generates bursty workload to trigger scaling"
        ],
    },
    {
        "folder": "Pipeline Factory",
        "title": "Natural Language to SQL Pipeline Factory",
        "bullets": [
            "Streamlit app uses Cortex to generate validated SQL",
            "Writes SQL into PIPELINE_CONFIG (db-level target DT)",
            "RUN_PIPELINE_FACTORY creates/refreshes Dynamic Tables",
            "Searchable DB/Schema, allowed tables multi-select"
        ],
    },
    {
        "folder": "Query Pattern Optimizer",
        "title": "Query Pattern Optimizer",
        "bullets": [
            "Stages QUERY_HISTORY into TECHUP.QPO_AUDIT.QUERY_HISTORY_STG",
            "Aggregates patterns and flags heavy scans/spillage",
            "Recommendations DT emits suggested DDL actions",
            "Executor procedure applies reviewed actions via task"
        ],
    },
    {
        "folder": "Performance Monitor",
        "title": "Self-Optimizing Performance Monitor",
        "bullets": [
            "Stages WAREHOUSE_METERING into TECHUP.AUDIT.WAREHOUSE_METERING_STG",
            "WAREHOUSE_PERFORMANCE_DT joins query + metering signals",
            "OPTIMIZATION_RECOMMENDATIONS_DT proposes improvements",
            "PENDING_DDL_ACTIONS_DT + RUN_PENDING_ACTIONS() for automation"
        ],
    },
]


def create_one_slide_pptx(title: str, bullets: list[str], out_path: str) -> None:
    prs = Presentation()
    slide_layout = prs.slide_layouts[1]  # Title and Content
    slide = prs.slides.add_slide(slide_layout)
    slide.shapes.title.text = title

    body_shape = slide.placeholders[1]
    tf = body_shape.text_frame
    tf.clear()
    for i, b in enumerate(bullets):
        if i == 0:
            p = tf.paragraphs[0]
        else:
            p = tf.add_paragraph()
        p.text = b
        p.level = 0
        for run in p.runs:
            run.font.size = Pt(18)

    prs.save(out_path)


def main() -> None:
    for spec in slides_spec:
        folder = os.path.join(ROOT, spec["folder"])
        os.makedirs(folder, exist_ok=True)
        out_path = os.path.join(folder, "overview.pptx")
        create_one_slide_pptx(spec["title"], spec["bullets"], out_path)
        print(f"Wrote {out_path}")


if __name__ == "__main__":
    main()


