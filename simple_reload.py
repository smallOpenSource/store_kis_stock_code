#!/usr/bin/env python3
"""
KIS 데이터 재적재 스크립트
kis_download 폴더의 파일 변경 시 전체 데이터를 재적재합니다.
"""

import logging
import sys
from datetime import datetime
from reload_data import main as reload_main

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/kis_reload_simple.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger('simple_reload')

def main():
    """간단한 재적재 실행"""
    try:
        logger.info("=" * 60)
        logger.info("KIS 데이터 간단 재적재 시작")
        logger.info("=" * 60)
        
        start_time = datetime.now()
        
        # 기존 reload_data.py의 main 함수 호출
        success = reload_main()
        
        end_time = datetime.now()
        elapsed = end_time - start_time
        
        logger.info("=" * 60)
        if success:
            logger.info("✅ KIS 데이터 재적재 성공!")
        else:
            logger.error("❌ KIS 데이터 재적재 실패!")
            
        logger.info(f"⏱️  소요시간: {elapsed}")
        logger.info("=" * 60)
        
        return success
        
    except Exception as e:
        logger.error(f"재적재 중 예상치 못한 오류: {e}", exc_info=True)
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
