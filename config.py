import os
from dotenv import load_dotenv

# 환경 변수 로드
load_dotenv()

# 데이터베이스 설정
DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': int(os.getenv('DB_PORT', 3306)),
    'user': os.getenv('DB_USER', 'root'),
    'password': os.getenv('DB_PASSWORD', ''),
    'database': os.getenv('DB_DATABASE', 'etl'),
    'charset': os.getenv('DB_CHARSET', 'utf8mb4')
}

# 경로 설정
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, 'kis_download')
FILE_SETS_CSV = os.path.join(DATA_DIR, 'kis_file_sets.csv')

# 배치 처리 크기
BATCH_SIZE = int(os.getenv('BATCH_SIZE', 1000))
