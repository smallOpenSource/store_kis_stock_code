ㅎimport pandas as pd
import struct
import os
import logging
from datetime import datetime
from .base_parser import BaseParser

logger = logging.getLogger('elw_parser')

class ELWParser(BaseParser):
    """ELW 마스터 파일 파서"""
    
    def __init__(self, master_file, header_file, market_type):
        super().__init__(master_file, header_file, market_type)
        self.record_size = 300  # ELW 레코드 크기
        self.data = None
    
    def parse(self):
        """ELW 마스터 파일 파싱"""
        extracted_file = self.extract_if_zip()
        
        result = []
        record_count = 0
        
        try:
            with open(extracted_file, 'rb') as f:
                file_data = f.read()
                total_records = len(file_data) // self.record_size
                
                logger.info(f"ELW 마스터 파일 파싱 시작: 총 {total_records}개 레코드")
                
                for i in range(total_records):
                    try:
                        offset = i * self.record_size
                        buffer = file_data[offset:offset+self.record_size]
                        
                        if len(buffer) < self.record_size:
                            break
                        
                        # 기본 ELW 정보
                        elw_code = self.safe_decode(buffer[0:12])
                        short_code = self.safe_decode(buffer[12:18])
                        elw_name = self.safe_decode(buffer[18:58])
                        
                        # 기초자산 정보
                        underlying_code = self.safe_decode(buffer[58:70])
                        underlying_asset = self.safe_decode(buffer[70:110])
                        
                        # 권리구분 및 행사가격
                        exercise_type_code = self.safe_decode(buffer[110:111])
                        exercise_type = '콜' if exercise_type_code == '1' else '풋' if exercise_type_code == '2' else None
                        
                        strike_price_str = self.safe_decode(buffer[111:126])
                        
                        # 만기일
                        maturity_date_str = self.safe_decode(buffer[126:134])
                        
                        # 상장일
                        listing_date_str = self.safe_decode(buffer[134:142])
                        
                        # 발행사코드
                        issuer_code = self.safe_decode(buffer[142:150])
                        
                        # 유효성 검증
                        if not elw_code or len(elw_code.strip()) < 6:
                            continue
                            
                        if not elw_name or elw_name.strip() == '':
                            continue
                        
                        # ELW 단축코드 처리: NULL이면 instrument_code에서 생성
                        processed_short_code = short_code.strip() if short_code and short_code.strip() else None
                        if not processed_short_code:
                            # ELW 코드에서 뒤 6자리를 단축코드로 사용
                            elw_code_clean = elw_code.strip()
                            if len(elw_code_clean) >= 6:
                                processed_short_code = elw_code_clean[-6:]
                        
                        # 결과에 추가
                        record = {
                            'instrument_code': elw_code.strip(),
                            'short_code': processed_short_code,
                            'instrument_name': elw_name.strip(),
                            'instrument_name_eng': None,
                            'instrument_type': 'ELW',
                            'instrument_subtype': exercise_type,
                            'market_type': self.market_type,
                            'country_code': 'KOR',
                            'currency_code': 'KRW',
                            'listing_date': self.parse_date(listing_date_str),
                            'maturity_date': self.parse_date(maturity_date_str),
                            'underlying_code': underlying_code.strip() if underlying_code else None,
                            'underlying_asset': underlying_asset.strip() if underlying_asset else None,
                            'exercise_price': self.parse_number(strike_price_str, is_float=True),
                            'issuer_code': issuer_code.strip() if issuer_code else None,
                            'is_elw': 'Y',
                            'is_etf': 'N',
                            'is_etn': 'N',
                            'is_warning': 'N',
                            'is_caution': 'N',
                            'is_risk': 'N',
                            'is_managed': 'N',
                            'is_credit_available': 'N',
                            'is_foreign': 'N',
                            'created_at': pd.Timestamp.now(),
                            'updated_at': pd.Timestamp.now()
                        }
                        result.append(record)
                        
                    except Exception as e:
                        logger.warning(f"ELW 레코드 {i} 파싱 오류: {e}")
                        continue
                    
                    # 정기적으로 진행 상황 로깅
                    if (i+1) % 500 == 0 or i+1 == total_records:
                        logger.info(f"  {i+1}/{total_records} 레코드 파싱 완료")
        
        except Exception as e:
            logger.error(f"ELW 파일 읽기 오류: {e}")
            raise
        
        logger.info(f"ELW 마스터 파일 파싱 완료: {len(result)}개 레코드")
        return result
    
    def transform(self, parsed_data):
        """데이터 변환 및 정제"""
        if not parsed_data:
            logger.warning("ELW 변환할 데이터 없음")
            return pd.DataFrame()
            
        df = pd.DataFrame(parsed_data)
        
        # 중복 제거
        df = df.drop_duplicates(subset=['instrument_code'])
        
        # 데이터 타입 정리
        df['instrument_code'] = df['instrument_code'].astype(str)
        df['instrument_name'] = df['instrument_name'].astype(str)
        df['market_type'] = df['market_type'].astype(str)
        
        logger.info(f"ELW 변환 완료: {len(df)}개 유효 레코드")
        
        return df
