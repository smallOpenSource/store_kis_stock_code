import logging
import pandas as pd
import sys
import os

# 상위 디렉토리를 Python 경로에 추가
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from db_utils import get_connection

logger = logging.getLogger('sample_validator')

class SampleValidator:
    """샘플 데이터 검증기"""
    
    def validate_known_instruments(self):
        """알려진 종목들의 존재 확인"""
        known_stocks = [
            {'code': '005930', 'name': '삼성전자', 'market': 'KOSPI'},
            {'code': '000660', 'name': 'SK하이닉스', 'market': 'KOSPI'},
            {'code': '035420', 'name': 'NAVER', 'market': 'KOSPI'},
            {'code': '051910', 'name': 'LG화학', 'market': 'KOSPI'},
            {'code': '207940', 'name': '삼성바이오로직스', 'market': 'KOSPI'},
            {'code': '373220', 'name': 'LG에너지솔루션', 'market': 'KOSPI'},
            {'code': '122870', 'name': '와이지엔터테인먼트', 'market': 'KOSDAQ'},
            {'code': '247540', 'name': '에코프로비엠', 'market': 'KOSDAQ'},
            {'code': '086520', 'name': '에코프로', 'market': 'KOSDAQ'},
        ]
        
        results = []
        conn = get_connection()
        try:
            with conn.cursor() as cursor:
                for stock in known_stocks:
                    cursor.execute("""
                        SELECT instrument_code, instrument_name, market_type, alias_names
                        FROM instruments
                        WHERE instrument_code = %s
                    """, (stock['code'],))
                    
                    result = cursor.fetchone()
                    if result:
                        found_code, found_name, found_market, aliases = result
                        
                        # 종목명 매칭 검사 (부분 일치 허용)
                        name_match = (
                            stock['name'] in found_name or 
                            found_name in stock['name'] or
                            (aliases and stock['name'] in aliases)
                        )
                        
                        is_valid = (
                            found_code == stock['code'] and
                            name_match and
                            found_market == stock['market']
                        )
                        
                        results.append({
                            'expected': stock,
                            'found': {
                                'code': found_code,
                                'name': found_name,
                                'market': found_market,
                                'aliases': aliases
                            },
                            'result': is_valid,
                            'description': f"대표 종목 {stock['code']} 검증"
                        })
                        
                        if is_valid:
                            logger.info(f"✓ {stock['code']} {stock['name']} 검증 성공")
                        else:
                            logger.warning(f"✗ {stock['code']} {stock['name']} 검증 실패 - 발견된 이름: {found_name}")
                    else:
                        results.append({
                            'expected': stock,
                            'found': None,
                            'result': False,
                            'description': f"대표 종목 {stock['code']} 검증"
                        })
                        logger.error(f"✗ {stock['code']} {stock['name']} 종목을 찾을 수 없음")
        finally:
            conn.close()
        
        return results
    
    def validate_search_functionality(self):
        """검색 기능 검증"""
        search_tests = [
            {
                'query': '삼성',
                'expected_min_results': 5,
                'should_include': ['005930'],  # 삼성전자
                'description': '삼성 관련 종목 검색'
            },
            {
                'query': 'SK',
                'expected_min_results': 3,
                'should_include': ['000660'],  # SK하이닉스
                'description': 'SK 관련 종목 검색'
            },
            {
                'query': 'LG',
                'expected_min_results': 3,
                'should_include': ['051910'],  # LG화학
                'description': 'LG 관련 종목 검색'
            },
            {
                'query': '전자',
                'expected_min_results': 10,
                'should_include': ['005930'],  # 삼성전자
                'description': '전자 관련 종목 검색'
            }
        ]
        
        results = []
        conn = get_connection()
        try:
            with conn.cursor() as cursor:
                for test in search_tests:
                    query = test['query']
                    
                    # 이름으로 검색
                    cursor.execute("""
                        SELECT instrument_code, instrument_name, market_type
                        FROM instruments
                        WHERE instrument_name LIKE %s
                           OR alias_names LIKE %s
                        ORDER BY 
                            CASE WHEN instrument_name LIKE %s THEN 1 ELSE 2 END,
                            market_type,
                            instrument_name
                        LIMIT 100
                    """, (f'%{query}%', f'%{query}%', f'{query}%'))
                    
                    search_results = cursor.fetchall()
                    found_codes = [result[0] for result in search_results]
                    
                    # 검증
                    min_check = len(search_results) >= test['expected_min_results']
                    include_check = all(code in found_codes for code in test['should_include'])
                    
                    results.append({
                        'query': query,
                        'found_count': len(search_results),
                        'expected_min': test['expected_min_results'],
                        'should_include': test['should_include'],
                        'found_codes': found_codes[:10],  # 상위 10개만
                        'sample_results': [f"{r[0]} {r[1]}" for r in search_results[:5]],
                        'min_check': min_check,
                        'include_check': include_check,
                        'result': min_check and include_check,
                        'description': test['description']
                    })
                    
                    if min_check and include_check:
                        logger.info(f"✓ '{query}' 검색: {len(search_results)}개 결과")
                    else:
                        logger.warning(f"✗ '{query}' 검색 실패: {len(search_results)}개 결과 (최소 {test['expected_min_results']}개 필요)")
        finally:
            conn.close()
        
        return results
    
    def validate_etf_samples(self):
        """ETF 샘플 검증"""
        conn = get_connection()
        try:
            with conn.cursor() as cursor:
                # ETF 종목 수 확인
                cursor.execute("""
                    SELECT COUNT(*) as etf_count
                    FROM instruments
                    WHERE is_etf = 'Y' OR instrument_type = 'ETF'
                """)
                etf_count = cursor.fetchone()[0]
                
                # 대표적인 ETF 검색
                cursor.execute("""
                    SELECT instrument_code, instrument_name, market_type
                    FROM instruments
                    WHERE (is_etf = 'Y' OR instrument_type = 'ETF')
                      AND instrument_name LIKE '%코스피%'
                    LIMIT 5
                """)
                etf_samples = cursor.fetchall()
                
                validation_result = {
                    'etf_count': etf_count,
                    'etf_samples': [f"{r[0]} {r[1]}" for r in etf_samples],
                    'result': etf_count > 0,
                    'description': 'ETF 종목 존재 검증'
                }
                
                if etf_count > 0:
                    logger.info(f"✓ ETF 종목 {etf_count}개 발견")
                else:
                    logger.warning("✗ ETF 종목을 찾을 수 없음")
                
                return validation_result
        finally:
            conn.close()
    
    def validate_market_coverage(self):
        """시장 커버리지 검증"""
        conn = get_connection()
        try:
            with conn.cursor() as cursor:
                # 시장별 종목 수
                cursor.execute("""
                    SELECT market_type, COUNT(*) as count
                    FROM instruments
                    GROUP BY market_type
                    ORDER BY count DESC
                """)
                market_coverage = cursor.fetchall()
                
                expected_markets = ['KOSPI', 'KOSDAQ', 'KONEX']
                found_markets = [row[0] for row in market_coverage]
                
                coverage_check = all(market in found_markets for market in expected_markets)
                
                validation_result = {
                    'market_coverage': {row[0]: row[1] for row in market_coverage},
                    'expected_markets': expected_markets,
                    'found_markets': found_markets,
                    'result': coverage_check,
                    'description': '주요 시장 커버리지 검증'
                }
                
                if coverage_check:
                    logger.info("✓ 주요 시장 (KOSPI, KOSDAQ, KONEX) 모두 커버됨")
                    for market, count in validation_result['market_coverage'].items():
                        logger.info(f"  {market}: {count:,}개")
                else:
                    missing_markets = set(expected_markets) - set(found_markets)
                    logger.warning(f"✗ 누락된 시장: {missing_markets}")
                
                return validation_result
        finally:
            conn.close()
