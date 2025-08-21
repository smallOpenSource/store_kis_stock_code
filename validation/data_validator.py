import logging
import pandas as pd
import sys
import os

# 상위 디렉토리를 Python 경로에 추가
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from db_utils import get_connection, execute_query
from validation.comprehensive_validator import ComprehensiveValidator

logger = logging.getLogger('data_validator')

class DataValidator:
    """데이터 검증 클래스 (기존 호환성을 위해 유지)"""
    
    def __init__(self):
        self.comprehensive_validator = ComprehensiveValidator()
    
    def validate_record_counts(self):
        """각 테이블의 레코드 수 검증"""
        tables = [
            'instruments', 'member_code', 'sector_code', 
            'theme_code'
        ]
        
        results = {}
        conn = get_connection()
        try:
            with conn.cursor() as cursor:
                for table in tables:
                    try:
                        cursor.execute(f"SELECT COUNT(*) FROM {table}")
                        count = cursor.fetchone()[0]
                        results[table] = count
                        logger.info(f"{table} 테이블: {count:,}개 레코드")
                    except Exception as e:
                        logger.error(f"{table} 테이블 카운트 오류: {e}")
                        results[table] = 0
        finally:
            conn.close()
        
        # instruments 테이블에는 반드시 데이터가 있어야 함
        success = results.get('instruments', 0) > 0
        if success:
            logger.info("기본 테이블 검증 통과")
        else:
            logger.warning("instruments 테이블에 데이터가 없습니다.")
        
        return success, results
    
    def validate_sample_queries(self):
        """샘플 쿼리 실행 검증 (기존 호환성)"""
        queries = [
            # 1. 시장별 종목 수
            {
                'name': '시장별 종목 수',
                'query': '''
                    SELECT market_type, instrument_type, COUNT(*) AS cnt
                    FROM instruments 
                    GROUP BY market_type, instrument_type
                    ORDER BY market_type, instrument_type
                '''
            },
            
            # 2. 삼성 관련 종목 조회
            {
                'name': '삼성 관련 종목',
                'query': '''
                    SELECT instrument_code, short_code, instrument_name, market_type
                    FROM instruments 
                    WHERE instrument_name LIKE '%삼성%' 
                    LIMIT 10
                '''
            }
        ]
        
        success = True
        conn = get_connection()
        try:
            with conn.cursor() as cursor:
                for query_info in queries:
                    try:
                        cursor.execute(query_info['query'])
                        result = cursor.fetchall()
                        logger.info(f"{query_info['name']}: {len(result)}개 결과")
                        
                        # 결과 일부 출력
                        if result:
                            for i, row in enumerate(result[:3]):
                                logger.info(f"  {i+1}: {row}")
                            if len(result) > 3:
                                logger.info(f"  ... (총 {len(result)}개)")
                                
                    except Exception as e:
                        logger.error(f"{query_info['name']} 쿼리 실패: {e}")
                        success = False
        finally:
            conn.close()
        
        return success
    
    def validate_all(self):
        """모든 검증 실행 (통합 검증기 사용)"""
        logger.info("=" * 50)
        logger.info("데이터 검증 시작 (통합 검증 모드)")
        logger.info("=" * 50)
        
        try:
            # 파일 매핑 로드
            from config import FILE_SETS_CSV
            file_mapping = pd.read_csv(FILE_SETS_CSV, encoding='utf-8')
            
            # 통합 검증 실행
            validation_result = self.comprehensive_validator.run_full_validation(file_mapping)
            
            # 결과 저장
            self.comprehensive_validator.save_report('validation_report.json')
            
            # 전체 결과
            overall_result = validation_result['overall_result']
            success = overall_result['overall_status'] in ['EXCELLENT', 'GOOD', 'ACCEPTABLE']
            
            logger.info("=" * 50)
            if success:
                logger.info("통합 검증 통과")
            else:
                logger.warning("통합 검증 실패")
            logger.info("=" * 50)
            
            return success
            
        except Exception as e:
            logger.error(f"검증 실행 중 오류: {e}")
            return False
