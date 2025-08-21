import pandas as pd
import os
from .base_parser import BaseParser
import logging

logger = logging.getLogger('member_parser')

class MemberParser(BaseParser):
    """회원사코드 마스터 파일 파서"""
    
    def __init__(self, master_file, header_file=None, market_type='회원사코드'):
        super().__init__(master_file, header_file, market_type)
        self.record_size = 50  # 회원사코드 레코드 크기
    
    def parse(self):
        """회원사코드 마스터 파일 파싱"""
        if not os.path.exists(self.master_file):
            logger.error(f"파일이 존재하지 않음: {self.master_file}")
            return []
            
        result = []
        with open(self.master_file, 'rb') as f:
            file_data = f.read()
            total_records = len(file_data) // self.record_size
            
            logger.info(f"회원사코드 마스터 파일 파싱 시작: 총 {total_records}개 레코드")
            
            for i in range(total_records):
                offset = i * self.record_size
                buffer = file_data[offset:offset+self.record_size]
                
                if len(buffer) < self.record_size:
                    break
                
                # 회원사코드 (0-5)
                member_code = buffer[0:5].decode('cp949', errors='ignore').strip()
                
                # 회원사명 (5-25)
                member_name = buffer[5:25].decode('cp949', errors='ignore').strip()
                
                # 회원사약어명 (25-35)
                member_abbr = buffer[25:35].decode('cp949', errors='ignore').strip()
                
                # 회원사영문명 (35-47)
                member_eng_name = buffer[35:47].decode('cp949', errors='ignore').strip()
                
                # 회원사구분 (47-48)
                member_type_code = buffer[47:48].decode('cp949', errors='ignore').strip()
                member_type = self._get_member_type(member_type_code)
                
                # 등록일 (48-56)
                reg_date_str = buffer[48:56].decode('cp949', errors='ignore').strip()
                
                record = {
                    'member_code': member_code,
                    'member_name': member_name,
                    'member_abbr': member_abbr,
                    'member_eng_name': member_eng_name,
                    'member_type': member_type,
                    'reg_date': reg_date_str
                }
                result.append(record)
        
        logger.info(f"회원사코드 마스터 파일 파싱 완료: {len(result)}개 레코드")
        return result
    
    def _get_member_type(self, type_code):
        """회원사구분코드를 텍스트로 변환"""
        types = {
            '1': '증권',
            '2': '선물',
            '3': '증권+선물',
            '4': '은행',
            '5': '보험',
            '6': '종금',
            '7': '기타'
        }
        return types.get(type_code, '기타')
    
    def transform(self, parsed_data):
        """데이터 변환 및 정제"""
        if not parsed_data:
            return pd.DataFrame()
            
        df = pd.DataFrame(parsed_data)
        
        # 날짜 변환
        if 'reg_date' in df.columns:
            df['reg_date'] = df['reg_date'].apply(
                lambda x: self.parse_date(x) if x else None
            )
        
        return df
