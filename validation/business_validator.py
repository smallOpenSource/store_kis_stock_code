import logging
import pandas as pd
import sys
import os

# 상위 디렉토리를 Python 경로에 추가
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from db_utils import get_connection

logger = logging.getLogger('business_validator')

class BusinessValidator:
    """비즈니스 로직 검증기"""
    
    def validate_market_distribution(self):
        """시장별 종목 분포 검증"""
        conn = get_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT market_type, instrument_type, COUNT(*) as count
                    FROM instruments
                    GROUP BY market_type, instrument_type
                    ORDER BY market_type, instrument_type
                """)
                results = cursor.fetchall()
                
                distribution = {}
                for row in results:
                    market = row[0]
                    inst_type = row[1]
                    count = row[2]
                    
                    if market not in distribution:
                        distribution[market] = {}
                    distribution[market][inst_type] = count
                
                # 예상되는 최소 종목 수 검증
                expected_minimums = {
                    'KOSPI': {'STOCK': 700, 'ETF': 100},    # 코스피 주식 및 ETF 최소 예상 수
                    'KOSDAQ': {'STOCK': 1300},               # 코스닥 주식 최소 예상 수
                    'KONEX': {'STOCK': 100},                 # 코넥스 주식 최소 예상 수
                }
                
                validation_results = []
                for market, types in expected_minimums.items():
                    for inst_type, min_count in types.items():
                        actual_count = distribution.get(market, {}).get(inst_type, 0)
                        is_valid = actual_count >= min_count
                        validation_results.append({
                            'check': f'{market}_{inst_type}_minimum_count',
                            'expected_minimum': min_count,
                            'actual_count': actual_count,
                            'result': is_valid,
                            'description': f'{market} {inst_type} 최소 종목 수 검증'
                        })
                        
                        if is_valid:
                            logger.info(f"✓ {market} {inst_type}: {actual_count:,}개 (최소 {min_count:,}개 이상)")
                        else:
                            logger.warning(f"✗ {market} {inst_type}: {actual_count:,}개 (최소 {min_count:,}개 필요)")
                
                return validation_results, distribution
        finally:
            conn.close()
    
    def validate_code_formats(self):
        """종목코드 형식 검증"""
        checks = []
        conn = get_connection()
        try:
            with conn.cursor() as cursor:
                # 1. 국내 주식 코드 형식 (6자리 숫자)
                cursor.execute("""
                    SELECT 'domestic_stock_code_format' as check_name,
                           COUNT(*) as invalid_count
                    FROM instruments
                    WHERE market_type IN ('KOSPI', 'KOSDAQ', 'KONEX')
                      AND instrument_type = 'STOCK'
                      AND (LENGTH(instrument_code) != 6 
                           OR instrument_code NOT REGEXP '^[0-9]{6}$')
                """)
                result = cursor.fetchone()
                checks.append({
                    'check': result[0],
                    'result': result[1] == 0,
                    'invalid_count': result[1],
                    'description': '국내 주식 코드 형식 검증 (6자리 숫자)'
                })
                
                # 2. 단축코드 길이 검사
                cursor.execute("""
                    SELECT 'short_code_length' as check_name,
                           COUNT(*) as invalid_count
                    FROM instruments
                    WHERE short_code IS NOT NULL 
                      AND (LENGTH(short_code) < 3 OR LENGTH(short_code) > 12)
                """)
                result = cursor.fetchone()
                checks.append({
                    'check': result[0],
                    'result': result[1] == 0,
                    'invalid_count': result[1],
                    'description': '단축코드 길이 검증 (3-12자)'
                })
                
                # 3. 종목명 길이 검사
                cursor.execute("""
                    SELECT 'instrument_name_length' as check_name,
                           COUNT(*) as invalid_count
                    FROM instruments
                    WHERE instrument_name IS NOT NULL 
                      AND (LENGTH(instrument_name) < 2 OR LENGTH(instrument_name) > 100)
                """)
                result = cursor.fetchone()
                checks.append({
                    'check': result[0],
                    'result': result[1] == 0,
                    'invalid_count': result[1],
                    'description': '종목명 길이 검증 (2-100자)'
                })
                
                # 4. 시장 유형 유효성 검사
                cursor.execute("""
                    SELECT 'market_type_validity' as check_name,
                           COUNT(*) as invalid_count
                    FROM instruments
                    WHERE market_type NOT IN ('KOSPI', 'KOSDAQ', 'KONEX', 'NYSE', 'NASDAQ', 'TSE', 'HKEX', 'SSE', 'ELW')
                """)
                result = cursor.fetchone()
                checks.append({
                    'check': result[0],
                    'result': result[1] == 0,
                    'invalid_count': result[1],
                    'description': '시장 유형 유효성 검증'
                })
                
        finally:
            conn.close()
        
        return checks
    
    def validate_alias_quality(self):
        """별칭 품질 검증"""
        conn = get_connection()
        try:
            with conn.cursor() as cursor:
                # 별칭이 있는 종목 비율
                cursor.execute("""
                    SELECT 
                        COUNT(*) as total_instruments,
                        COUNT(CASE WHEN alias_names IS NOT NULL AND alias_names != '' THEN 1 END) as with_alias,
                        ROUND(COUNT(CASE WHEN alias_names IS NOT NULL AND alias_names != '' THEN 1 END) * 100.0 / COUNT(*), 2) as alias_percentage
                    FROM instruments
                """)
                result = cursor.fetchone()
                
                # 별칭 개수 통계
                cursor.execute("""
                    SELECT 
                        AVG(CHAR_LENGTH(alias_names) - CHAR_LENGTH(REPLACE(alias_names, ',', '')) + 1) as avg_alias_count
                    FROM instruments
                    WHERE alias_names IS NOT NULL AND alias_names != ''
                """)
                avg_result = cursor.fetchone()
                
                alias_stats = {
                    'total_instruments': result[0],
                    'with_alias': result[1],
                    'alias_percentage': result[2],
                    'avg_alias_count': round(avg_result[0] if avg_result[0] else 0, 2)
                }
                
                logger.info(f"별칭 통계: 전체 {alias_stats['total_instruments']:,}개 중 {alias_stats['with_alias']:,}개({alias_stats['alias_percentage']}%)가 별칭 보유")
                logger.info(f"평균 별칭 개수: {alias_stats['avg_alias_count']}개")
                
                return alias_stats
        finally:
            conn.close()
    
    def validate_data_completeness(self):
        """데이터 완성도 검증"""
        checks = []
        conn = get_connection()
        try:
            with conn.cursor() as cursor:
                # 1. 상장일 정보 완성도
                cursor.execute("""
                    SELECT 'listing_date_completeness' as check_name,
                           COUNT(CASE WHEN listing_date IS NOT NULL THEN 1 END) as with_date,
                           COUNT(*) as total,
                           ROUND(COUNT(CASE WHEN listing_date IS NOT NULL THEN 1 END) * 100.0 / COUNT(*), 2) as percentage
                    FROM instruments
                    WHERE instrument_type = 'STOCK'
                """)
                result = cursor.fetchone()
                percentage = result[3] if result[3] is not None else 0.0
                checks.append({
                    'check': result[0],
                    'result': percentage >= 80.0,  # 80% 이상이면 통과
                    'with_data': result[1],
                    'total': result[2],
                    'percentage': percentage,
                    'description': '주식 상장일 정보 완성도 (80% 이상 목표)'
                })
                
                cursor.execute("""
                    SELECT 'industry_code_completeness' as check_name,
                           COUNT(CASE WHEN industry_code IS NOT NULL AND industry_code != '' THEN 1 END) as with_code,
                           COUNT(*) as total,
                           ROUND(COUNT(CASE WHEN industry_code IS NOT NULL AND industry_code != '' THEN 1 END) * 100.0 / COUNT(*), 2) as percentage
                    FROM instruments
                    WHERE instrument_type = 'STOCK' AND market_type IN ('KOSPI', 'KOSDAQ')
                """)
                result = cursor.fetchone()
                percentage = result[3] if result[3] is not None else 0.0
                checks.append({
                    'check': result[0],
                    'result': percentage >= 70.0,  # 70% 이상이면 통과
                    'with_data': result[1],
                    'total': result[2],
                    'percentage': percentage,
                    'description': '국내 주식 업종코드 정보 완성도 (70% 이상 목표)'
                })
                
        finally:
            conn.close()
        
        return checks
