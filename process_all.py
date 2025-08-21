#!/usr/bin/env python3
"""
KIS 데이터 전체 프로세스 실행
1. 데이터 재적재
2. 검증 및 마크다운 리포트 생성
"""

import logging
import sys
import os
from datetime import datetime

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/kis_process.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger('kis_process')

def main():
    """전체 프로세스 실행"""
    
    print("🚀 KIS 데이터 처리 시작")
    print("=" * 60)
    
    start_time = datetime.now()
    
    try:
        # 1. 데이터 재적재
        print("1️⃣  데이터 재적재 중...")
        from simple_reload import main as reload_main
        
        reload_success = reload_main()
        
        if not reload_success:
            print("❌ 데이터 재적재 실패!")
            return False
        
        print("✅ 데이터 재적재 성공!")
        print()
        
        # 2. 검증 및 리포트 생성
        print("2️⃣  검증 및 리포트 생성 중...")
        from validation_report import main as validation_main
        
        validation_success = validation_main()
        
        if not validation_success:
            print("❌ 검증 실패!")
            return False
            
        print("✅ 검증 및 리포트 생성 성공!")
        print()
        
        # 3. 결과 요약
        end_time = datetime.now()
        elapsed = end_time - start_time
        
        print("🎉 KIS 데이터 처리 완료!")
        print(f"⏱️  총 소요시간: {elapsed}")
        print()
        
        # 생성된 리포트 파일 목록
        report_files = [f for f in os.listdir('reports') if f.startswith('kis_validation_report_') and f.endswith('.md')]
        if report_files:
            latest_report = max(report_files)
            print(f"📄 마크다운 리포트: reports/{latest_report}")
        
        return True
        
    except Exception as e:
        logger.error(f"처리 중 예상치 못한 오류: {e}", exc_info=True)
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
