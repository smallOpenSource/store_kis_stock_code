import logging
import pandas as pd
import sys
import os

# 상위 디렉토리를 Python 경로에 추가
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import FILE_SETS_CSV
from validation.comprehensive_validator import ComprehensiveValidator

def setup_logging():
    """로깅 설정"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('kis_validation.log', encoding='utf-8'),
            logging.StreamHandler()
        ]
    )

def main():
    """검증 실행"""
    setup_logging()
    logger = logging.getLogger('validation_main')
    
    try:
        logger.info("[START] KIS 종목정보 데이터 검증 시작")
        
        # 파일 매핑 로드
        try:
            file_mapping = pd.read_csv(FILE_SETS_CSV, encoding='utf-8')
            logger.info(f"[FILES] 파일 매핑 정보 로드 완료: {len(file_mapping)}개 항목")
        except Exception as e:
            logger.error(f"❌ 파일 매핑 정보 로드 실패: {e}")
            return False
        
        # 통합 검증 실행
        validator = ComprehensiveValidator()
        validation_result = validator.run_full_validation(file_mapping)
        
        # 결과 저장
        validator.save_report('kis_validation_report.json')
        
        # 최종 결과 판정
        overall_result = validation_result['overall_result']
        summary = validator.get_summary()
        
        logger.info("\n" + "="*60)
        logger.info("📋 검증 결과 요약")
        logger.info("="*60)
        logger.info(f"상태: {summary['status']}")
        logger.info(f"성공률: {summary['success_rate']}%")
        logger.info(f"총 종목 수: {summary['total_instruments']:,}개")
        logger.info(f"검증 시간: {summary['validation_time']}")
        
        if overall_result['overall_status'] in ['EXCELLENT', 'GOOD']:
            logger.info("✅ 데이터 검증 성공!")
            return True
        elif overall_result['overall_status'] == 'ACCEPTABLE':
            logger.warning("⚠️  데이터 검증 완료 (일부 개선 필요)")
            return True
        else:
            logger.error("❌ 데이터 검증 실패!")
            logger.error(f"심각한 오류: {overall_result['critical_failures']}개")
            return False
            
    except Exception as e:
        logger.error(f"[ERROR] 검증 실행 중 오류: {e}", exc_info=True)
        return False

if __name__ == "__main__":
    success = main()
    if success:
        print("\n[SUCCESS] 검증이 성공적으로 완료되었습니다!")
        exit(0)
    else:
        print("\n[ERROR] 검증이 실패했습니다. 로그를 확인해주세요.")
        exit(1)
