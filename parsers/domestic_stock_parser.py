import pandas as pd
import numpy as np
import os
from datetime import datetime
from .base_parser import BaseParser
import logging

logger = logging.getLogger('domestic_stock_parser')

class DomesticStockParser(BaseParser):
    """국내주식 마스터 파일 파서"""
    
    def __init__(self, master_file, header_file, market_type):
        super().__init__(master_file, header_file, market_type)
        self.record_size = 320  # 코스피/코스닥 레코드 크기
    
    def parse(self):
        """국내주식 마스터 파일 파싱"""
        extracted_file = self.extract_if_zip()
        
        if not os.path.exists(extracted_file):
            logger.error(f"파일이 존재하지 않음: {extracted_file}")
            return []
            
        # 샘플 코드와 동일하게 텍스트 모드로 파싱
        result = []
        with open(extracted_file, mode="r", encoding="cp949") as f:
            lines = f.readlines()
            total_records = len(lines)
            
            logger.info(f"{self.market_type} 마스터 파일 파싱 시작: 총 {total_records:,}개 레코드")
            
            # 첫 라인 분석 (디버깅용)
            if lines:
                first_line = lines[0]
                logger.info(f"첫 라인 길이: {len(first_line)}")
                if len(first_line) >= 50:
                    sample_front = first_line[:50]
                    logger.info(f"첫 라인 앞 50바이트: {sample_front}")
            
            for i, line in enumerate(lines):  # 모든 레코드 파싱
                if len(line) < 228:  # 최소 길이 체크
                    continue
                    
                # 샘플 코드 방식으로 파싱
                front_part_len = len(line) - 228
                front_part = line[:front_part_len]
                back_part = line[front_part_len:]
                
                if len(front_part) < 21:  # 최소 필요 길이
                    continue
                
                # 앞부분 파싱
                short_code = front_part[0:9].rstrip()
                stock_code = front_part[9:21].rstrip() 
                stock_name = front_part[21:].strip()
                
                # 뒤쪽 고정폭 필드들 (228바이트) 파싱
                # 샘플 코드의 field_specs 순서대로
                offset = 0
                
                # 그룹코드(2) + 시가총액규모(1) + 지수업종대분류(4) + 지수업종중분류(4) + 지수업종소분류(4)
                group_code = back_part[offset:offset+2].strip()
                offset += 2
                market_cap_scale = back_part[offset:offset+1].strip()
                offset += 1
                industry_large = back_part[offset:offset+4].strip()
                offset += 4
                industry_medium = back_part[offset:offset+4].strip()
                offset += 4
                industry_small = back_part[offset:offset+4].strip()
                offset += 4
                
                # 업종코드는 대분류 코드 사용
                industry_code = industry_large
                
                # 나머지 필드들 스킵하여 필요한 위치로 이동
                # 제조업(1) + 저유동성(1) + ... + SRI(1) = 26개 1바이트 필드들 스킵
                offset += 26
                
                # 기준가(9) + 매매수량단위(5) + 시간외수량단위(5) + 거래정지(1) + 정리매매(1) + 관리종목(1)
                base_price = back_part[offset:offset+9].strip()
                offset += 9
                trade_unit = back_part[offset:offset+5].strip()
                offset += 5
                after_hours_unit = back_part[offset:offset+5].strip()
                offset += 5
                is_halt = back_part[offset:offset+1].strip() == 'Y'
                offset += 1
                is_clearing = back_part[offset:offset+1].strip() == 'Y'
                offset += 1
                is_managed = back_part[offset:offset+1].strip() == 'Y'
                offset += 1
                
                # 시장경고(2) + 경고예고(1) + 불성실공시(1) + 우회상장(1) + 락구분(2) + 액면변경(2) + 증자구분(2) + 증거금비율(3)
                market_warning = back_part[offset:offset+2].strip()
                is_warning = market_warning == '01'  # 01이 경고인지 확인 필요
                offset += 2
                offset += 8  # 나머지 스킵
                
                # 신용가능(1) + 신용기간(3) + 전일거래량(12) + 액면가(12) + 상장일자(8)
                offset += 1  # 신용가능 스킵
                offset += 3  # 신용기간 스킵
                offset += 12 # 전일거래량 스킵
                face_value_str = back_part[offset:offset+12].strip()
                face_value = self.parse_number(face_value_str) or 0
                offset += 12
                listing_date_str = back_part[offset:offset+8].strip()
                
                # 상장일 처리 개선
                parsed_listing_date = None
                if listing_date_str and listing_date_str != '00000000' and len(listing_date_str) == 8:
                    try:
                        # YYYYMMDD 형식으로 파싱
                        parsed_listing_date = f"{listing_date_str[:4]}-{listing_date_str[4:6]}-{listing_date_str[6:8]}"
                        # 유효한 날짜인지 검증
                        from datetime import datetime
                        datetime.strptime(parsed_listing_date, '%Y-%m-%d')
                    except:
                        parsed_listing_date = None
                
                # 상장일이 없으면 기본값 설정 (과거 날짜)
                if not parsed_listing_date:
                    parsed_listing_date = '1990-01-01'  # 기본 상장일
                
                # 상품유형 판별 (ETF 여부는 그룹코드나 종목명으로 판별)
                instrument_type = 'STOCK'  # 기본값
                if 'ETF' in stock_name or 'ETN' in stock_name or group_code in ['EF', 'EN']:
                    instrument_type = 'ETF'
                
                # 주요 종목 검증 (디버깅용)
                key_stocks = {
                    '005930': '삼성전자',
                    '000660': 'SK하이닉스',
                    '035420': 'NAVER',
                    '207940': '삼성바이오로직스',
                    '051910': 'LG화학'
                }
                
                if stock_code in key_stocks:
                    expected_name = key_stocks[stock_code]
                    if stock_name != expected_name:
                        logger.warning(f"주요 종목명 불일치: {stock_code} - 예상: {expected_name}, 실제: {stock_name}")
                        logger.info(f"문제 라인: {line[:100]}")
                    else:
                        logger.info(f"주요 종목 정상: {stock_code} - {stock_name}")
                
                # 기본값 설정
                stock_name_eng = ''  # 영문명은 별도 처리 필요
                is_etf = 'Y' if instrument_type == 'ETF' else 'N'
                is_etn = 'N'  # 현재는 사용하지 않음
                is_elw = 'N'  # 현재는 사용하지 않음
                
                # 결과에 추가
                record = {
                    'instrument_code': stock_code,
                    'short_code': short_code,
                    'instrument_name': stock_name,
                    'instrument_name_eng': stock_name_eng,
                    'instrument_type': instrument_type,
                    'market_type': self.market_type,
                    'country_code': 'KOR',
                    'currency_code': 'KRW',
                    'listing_date': parsed_listing_date,
                    'face_value': face_value,
                    'industry_code': industry_code,
                    'is_warning': 'Y' if is_warning else 'N',
                    'is_managed': 'Y' if is_managed else 'N',
                    'is_etf': is_etf,
                    'is_etn': is_etn,
                    'is_elw': is_elw
                }
                result.append(record)
                
                # 정기적으로 진행 상황 로깅
                if (i+1) % 500 == 0 or i+1 == total_records:
                    logger.info(f"  {i+1}/{total_records} 레코드 파싱 완료")
        
        logger.info(f"{self.market_type} 마스터 파일 파싱 완료: {len(result)}개 레코드")
        return result
    
    def transform(self, parsed_data):
        """데이터 변환 및 정제"""
        if not parsed_data:
            return pd.DataFrame()
            
        df = pd.DataFrame(parsed_data)
        
        # 날짜 변환 (예: 상장일)
        if 'listing_date' in df.columns:
            df['listing_date'] = df['listing_date'].apply(
                lambda x: self.parse_date(x) if x else None
            )
        
        # 플래그 필드 처리
        flag_fields = [
            'is_etf', 'is_etn', 'is_elw', 'is_reit', 'is_spac',
            'is_warning', 'is_risk', 'is_caution', 'is_managed', 
            'is_credit_available', 'is_foreign'
        ]
        
        for field in flag_fields:
            if field in df.columns:
                df[field] = df[field].apply(
                    lambda x: 'Y' if x == 'Y' else 'N'
                )
            elif field in ['is_reit', 'is_spac', 'is_risk', 'is_caution', 
                          'is_credit_available', 'is_foreign']:
                df[field] = 'N'  # 기본값
        
        # alias_names 생성
        df['alias_names'] = df.apply(
            lambda row: self._generate_aliases(row), axis=1
        )
        
        # 삼성전자 등 주요 종목 검증
        if self.market_type == '코스피':
            samsung = df[df['short_code'] == '005930']
            if len(samsung) == 1:
                logger.info(f"삼성전자 확인: {samsung.iloc[0]['instrument_name']}")
            else:
                logger.warning("삼성전자(005930) 종목을 찾을 수 없음")
        
        return df
    
    def _generate_aliases(self, row):
        """별칭 생성"""
        aliases = []
        
        # 기본 별칭: 종목명, 단축코드
        if pd.notna(row['instrument_name']):
            aliases.append(row['instrument_name'])
        if pd.notna(row['short_code']):
            aliases.append(row['short_code'])
        
        # 종목명에서 주식회사, (주) 등 제거한 약칭 추가
        if pd.notna(row['instrument_name']):
            name = row['instrument_name']
            if '주식회사' in name:
                aliases.append(name.replace('주식회사', '').strip())
            if '(주)' in name:
                aliases.append(name.replace('(주)', '').strip())
        
        # 영문명이 있으면 추가
        if pd.notna(row['instrument_name_eng']) and row['instrument_name_eng']:
            aliases.append(row['instrument_name_eng'])
        
        # 중복 제거 및 결합
        aliases = list(set(aliases))
        return ','.join(aliases)
