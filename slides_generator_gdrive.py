"""
Generate one-slide Google Slides decks for each project folder using Google APIs.

Setup:
1) Create a Google Cloud project and enable APIs: Slides API, Drive API.
2) Create OAuth client credentials (Desktop App) and download credentials.json.
3) Place credentials.json at repo root (same folder as this script).
4) First run will open a browser for OAuth; token.json will be saved for reuse.

Usage:
  pip install --quiet google-api-python-client google-auth-httplib2 google-auth-oauthlib
  python3 slides_generator_gdrive.py
"""
import os
from typing import List, Dict

from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build

SCOPES = [
    'https://www.googleapis.com/auth/drive.file',
    'https://www.googleapis.com/auth/presentations'
]

ROOT = os.path.dirname(os.path.abspath(__file__))

SLIDE_SPECS: List[Dict] = [
    {
        "folder": "Adaptive Right-Sizing",
        "title": "Adaptive Warehouse Right-Sizing",
        "bullets": [
            "Computes hour-by-hour policy from metering (credits_used)",
            "Dynamic Table: RIGHT_SIZING_POLICY_DT",
            "Executor: APPLY_RIGHT_SIZING() + task",
            "Seed workload to trigger scaling"
        ],
    },
    {
        "folder": "Pipeline Factory",
        "title": "Natural Language to SQL Pipeline Factory",
        "bullets": [
            "Cortex-driven SQL generation + validation",
            "Writes into PIPELINE_CONFIG (db-level DT target)",
            "RUN_PIPELINE_FACTORY creates/refreshes DTs",
            "Searchable DB/Schema; allowed tables multi-select"
        ],
    },
    {
        "folder": "Query Pattern Optimizer",
        "title": "Query Pattern Optimizer",
        "bullets": [
            "Stages QUERY_HISTORY → TECHUP.QPO_AUDIT.QUERY_HISTORY_STG",
            "Aggregates patterns; flags heavy scans/spillage",
            "Recommendations DT emits DDL suggestions",
            "Executor applies reviewed actions via task"
        ],
    },
    {
        "folder": "Performance Monitor",
        "title": "Self-Optimizing Performance Monitor",
        "bullets": [
            "Stages metering → TECHUP.AUDIT.WAREHOUSE_METERING_STG",
            "WAREHOUSE_PERFORMANCE_DT correlates query + metering",
            "OPTIMIZATION_RECOMMENDATIONS_DT proposes improvements",
            "PENDING_DDL_ACTIONS_DT + RUN_PENDING_ACTIONS()"
        ],
    },
]


def get_creds() -> Credentials:
    creds = None
    token_path = os.path.join(ROOT, 'token.json')
    cred_path = os.path.join(ROOT, 'credentials.json')
    if os.path.exists(token_path):
        creds = Credentials.from_authorized_user_file(token_path, SCOPES)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(cred_path, SCOPES)
            creds = flow.run_local_server(port=0)
        with open(token_path, 'w') as token:
            token.write(creds.to_json())
    return creds


def create_one_slide_presentation(slides_service, title: str, bullets: List[str]) -> str:
    # Create empty presentation
    pres = slides_service.presentations().create(body={"title": title}).execute()
    presentation_id = pres['presentationId']

    # Default presentation has a title slide; add title+body slide
    requests = [
        {
            "createSlide": {
                "slideLayoutReference": {"predefinedLayout": "TITLE_AND_BODY"}
            }
        }
    ]
    slides_service.presentations().batchUpdate(
        presentationId=presentation_id, body={"requests": requests}
    ).execute()

    # Insert title and bullets into the last slide
    pres = slides_service.presentations().get(presentationId=presentation_id).execute()
    slide = pres['slides'][-1]
    page_elements = slide['pageElements']

    # Find title and body shapes
    title_shape_id = None
    body_shape_id = None
    for el in page_elements:
        if 'shape' in el and 'title' in el['shape'].get('shapeType', '').lower():
            title_shape_id = el['objectId']
        if 'shape' in el and el['shape'].get('shapeType') == 'TEXT_BOX':
            body_shape_id = el['objectId']

    # Fallback: just pick first two shapes
    if not title_shape_id and page_elements:
        title_shape_id = page_elements[0]['objectId']
    if not body_shape_id and len(page_elements) > 1:
        body_shape_id = page_elements[1]['objectId']

    requests = []
    if title_shape_id:
        requests.append({
            "insertText": {
                "objectId": title_shape_id,
                "insertionIndex": 0,
                "text": title
            }
        })
    if body_shape_id:
        body_text = "\n".join([f"• {b}" for b in bullets])
        requests.append({
            "insertText": {
                "objectId": body_shape_id,
                "insertionIndex": 0,
                "text": body_text
            }
        })

    if requests:
        slides_service.presentations().batchUpdate(
            presentationId=presentation_id, body={"requests": requests}
        ).execute()

    return presentation_id


def main() -> None:
    creds = get_creds()
    slides_service = build('slides', 'v1', credentials=creds)
    drive_service = build('drive', 'v3', credentials=creds)

    for spec in SLIDE_SPECS:
        pres_id = create_one_slide_presentation(slides_service, spec['title'], spec['bullets'])
        link = f"https://docs.google.com/presentation/d/{pres_id}/edit"
        out_txt = os.path.join(ROOT, spec['folder'], 'slides_link.txt')
        os.makedirs(os.path.dirname(out_txt), exist_ok=True)
        with open(out_txt, 'w') as f:
            f.write(link + "\n")
        print(f"Created Google Slides for {spec['folder']}: {link}")


if __name__ == '__main__':
    main()


