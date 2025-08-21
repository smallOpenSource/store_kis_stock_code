import pandas as pd
import os
import logging
from .base_parser import BaseParser

logger = logging.getLogger('member_parser')

class MemberParser(BaseParser):
    """회원사 코드 파서"""
    
    def __init__(self, master_file, header_file=None):
        super().__init__(master_file, header_file, '회원사코드')
        self.record_size = 50  # 회원사 레코드 크기 (추정)
    
    def parse(self):
        """회원사 코드 파일 파싱"""
        extracted_file = self.extract_if_zip()
        
        result = []
        record_count = 0
        
        try:
            with open(extracted_file, 'rb') as f:
                while True:
                    buffer = f.read(self.record_size)
                    if not buffer or len(buffer) < self.record_size:
                        break
                    
                    record_count += 1
                    
                    try:
                        # 회원사코드 파싱 (추정 구조)
                        member_code = self.safe_decode(buffer[0:5])
                        member_name = self.safe_decode(buffer[5:45])
                        
                        # 유효성 검증
                        if not member_code or len(member_code.strip()) == 0:
                            continue
                            
                        if not member_name or member_name.strip() == '':
                            continue
                        
                        record = {
                            'member_code': member_code.strip(),
                            'member_name': member_name.strip(),
                            'is_active': 'Y',
                            'created_at': pd.Timestamp.now(),
                            'updated_at': pd.Timestamp.now()
                        }
                        result.append(record)
                        
                    except Exception as e:
                        logger.warning(f"레코드 {record_count} 파싱 오류: {e}")
                        continue
        
        except Exception as e:
            logger.error(f"회원사코드 파일 읽기 오류: {e}")
            raise
        
        logger.info(f"회원사코드 마스터 파일 파싱 완료: {len(result)}개 레코드")
        return result
    
    def transform(self, parsed_data):
        """데이터 변환 및 정제"""
        if not parsed_data:
            logger.warning("회원사코드 변환할 데이터 없음")
            return pd.DataFrame()
            
        df = pd.DataFrame(parsed_data)
        
        # 중복 제거
        df = df.drop_duplicates(subset=['member_code'])
        
        # 데이터 타입 정리
        df['member_code'] = df['member_code'].astype(str)
        df['member_name'] = df['member_name'].astype(str)
        
        logger.info(f"회원사코드 변환 완료: {len(df)}개 유효 레코드")
        
        return df
