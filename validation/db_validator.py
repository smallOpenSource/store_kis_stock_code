import logging
import pandas as pd
import sys
import os

# 상위 디렉토리를 Python 경로에 추가
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from db_utils import get_connection

logger = logging.getLogger('db_validator')

class DatabaseValidator:
    """데이터베이스 검증기"""
    
    def __init__(self):
        self.db_stats = {}
    
    def count_table_records(self):
        """각 테이블의 레코드 수 조회"""
        tables = [
            'instruments', 
            'member_code', 
            'sector_code', 
            'theme_code', 
            'instrument_theme_map'
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
        
        self.db_stats = results
        return results
    
    def check_data_types(self):
        """데이터 타입 및 제약조건 검증"""
        checks = []
        conn = get_connection()
        try:
            with conn.cursor() as cursor:
                # 1. Primary Key 중복 검사
                cursor.execute("""
                    SELECT 'instruments_pk_duplicate' as check_name,
                           COUNT(*) - COUNT(DISTINCT instrument_code) as duplicate_count
                    FROM instruments
                """)
                result = cursor.fetchone()
                checks.append({
                    'check': result[0],
                    'result': result[1] == 0,
                    'value': result[1],
                    'description': '종목코드 중복 검사'
                })
                
                # 2. 필수 필드 NULL 검사 (ELW 특성 고려)
                cursor.execute("""
                    SELECT 'instruments_required_fields' as check_name,
                           COUNT(*) as null_count
                    FROM instruments 
                    WHERE instrument_code IS NULL 
                       OR instrument_code = ''
                       OR instrument_name IS NULL 
                       OR instrument_name = ''
                       OR instrument_type IS NULL 
                       OR instrument_type = ''
                       OR market_type IS NULL
                       OR market_type = ''
                       OR (instrument_type != 'ELW' AND short_code IS NULL)  -- ELW 제외 단축코드 필수
                       OR (instrument_type != 'ELW' AND short_code = '')     -- ELW 제외 단축코드 필수
                """)
                result = cursor.fetchone()
                checks.append({
                    'check': result[0],
                    'result': result[1] == 0,
                    'value': result[1],
                    'description': '필수 필드 NULL/빈값 검사 (ELW 제외)'
                })
                
                # 3. 날짜 형식 검사
                cursor.execute("""
                    SELECT 'instruments_date_format' as check_name,
                           COUNT(*) as invalid_date_count
                    FROM instruments 
                    WHERE (listing_date IS NOT NULL AND listing_date < '1900-01-01')
                       OR (maturity_date IS NOT NULL AND maturity_date < '1900-01-01')
                       OR (listing_date IS NOT NULL AND listing_date > CURDATE() + INTERVAL 10 YEAR)
                """)
                result = cursor.fetchone()
                checks.append({
                    'check': result[0],
                    'result': result[1] == 0,
                    'value': result[1],
                    'description': '날짜 형식 유효성 검사'
                })
                
                # 4. 플래그 필드 값 검사
                cursor.execute("""
                    SELECT 'instruments_flag_values' as check_name,
                           COUNT(*) as invalid_flag_count
                    FROM instruments 
                    WHERE is_etf NOT IN ('Y', 'N')
                       OR is_warning NOT IN ('Y', 'N')
                       OR is_managed NOT IN ('Y', 'N')
                       OR is_elw NOT IN ('Y', 'N')
                       OR is_foreign NOT IN ('Y', 'N')
                """)
                result = cursor.fetchone()
                checks.append({
                    'check': result[0],
                    'result': result[1] == 0,
                    'value': result[1],
                    'description': '플래그 필드 값 검증 (Y/N)'
                })
                
                # 5. 숫자 필드 검사
                cursor.execute("""
                    SELECT 'instruments_numeric_fields' as check_name,
                           COUNT(*) as invalid_numeric_count
                    FROM instruments 
                    WHERE (face_value IS NOT NULL AND face_value < 0)
                       OR (issue_cnt IS NOT NULL AND issue_cnt < 0)
                       OR (strike_price IS NOT NULL AND strike_price < 0)
                """)
                result = cursor.fetchone()
                checks.append({
                    'check': result[0],
                    'result': result[1] == 0,
                    'value': result[1],
                    'description': '숫자 필드 유효성 검사 (음수 체크)'
                })
                
        finally:
            conn.close()
        
        return checks
    
    def check_referential_integrity(self):
        """참조 무결성 검사"""
        checks = []
        conn = get_connection()
        try:
            with conn.cursor() as cursor:
                # 1. 테마 매핑 테이블의 외래키 검사 (instruments)
                cursor.execute("""
                    SELECT 'theme_mapping_instrument_fk' as check_name,
                           COUNT(*) as orphan_count
                    FROM instrument_theme_map m
                    LEFT JOIN instruments i ON m.instrument_code = i.instrument_code
                    WHERE i.instrument_code IS NULL
                """)
                result = cursor.fetchone()
                checks.append({
                    'check': result[0],
                    'result': result[1] == 0,
                    'value': result[1],
                    'description': '테마매핑-종목 외래키 무결성'
                })
                
                # 2. 테마 매핑 테이블의 외래키 검사 (theme_code)
                cursor.execute("""
                    SELECT 'theme_mapping_theme_fk' as check_name,
                           COUNT(*) as orphan_count
                    FROM instrument_theme_map m
                    LEFT JOIN theme_code t ON m.theme_code = t.theme_code
                    WHERE t.theme_code IS NULL
                """)
                result = cursor.fetchone()
                checks.append({
                    'check': result[0],
                    'result': result[1] == 0,
                    'value': result[1],
                    'description': '테마매핑-테마코드 외래키 무결성'
                })
                
        except Exception as e:
            logger.warning(f"참조 무결성 검사 중 오류 (테이블이 없을 수 있음): {e}")
        finally:
            conn.close()
        
        return checks
