from pathlib import Path
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).parent.parent

load_dotenv(verbose=True)

GLOBAL_TERRORISM = PROJECT_ROOT / 'data' / 'globalterrorismdb_0718dist.csv'
WORLDWIDE_TERRORISM_INCIDENTS = PROJECT_ROOT / 'data' / 'Database_of_Worldwide_Terrorism_Incidents.csv'