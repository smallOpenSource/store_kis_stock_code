import os
import zipfile
import pandas as pd
import logging
from datetime import datetime
from config import DATA_DIR

logger = logging.getLogger('base_parser')

class BaseParser:
    """기본 파서 클래스"""
    
    def __init__(self, master_file, header_file=None, market_type=None):
        self.master_file = os.path.join(DATA_DIR, master_file)
        self.header_file = os.path.join(DATA_DIR, header_file) if header_file else None
        self.market_type = market_type
        self.data = None
        self.record_size = None
    
    def extract_if_zip(self):
        """ZIP 파일인 경우 압축 해제"""
        if self.master_file.endswith('.zip'):
            try:
                base_dir = os.path.dirname(self.master_file)
                with zipfile.ZipFile(self.master_file, 'r') as zip_ref:
                    extracted_files = zip_ref.namelist()
                    zip_ref.extractall(base_dir)
                    
                # 압축 해제된 파일 중 .mst 또는 .cod 파일 찾기
                for file in extracted_files:
                    ext = os.path.splitext(file)[1].lower()
                    if ext in ['.mst', '.cod']:
                        extracted_file = os.path.join(base_dir, file)
                        logger.info(f"압축 해제 완료: {extracted_file}")
                        return extracted_file
                
                raise ValueError(f"압축 파일 {self.master_file}에서 마스터 파일을 찾을 수 없습니다.")
            except Exception as e:
                logger.error(f"ZIP 파일 압축 해제 오류: {e}")
                raise
        
        return self.master_file
    
    def safe_decode(self, binary_data, encoding='cp949'):
        """바이너리 데이터를 텍스트로 디코딩 (alias for decode_text)"""
        return self.decode_text(binary_data, encoding)
    
    def decode_text(self, binary_data, encoding='cp949'):
        """바이너리 데이터를 텍스트로 디코딩"""
        try:
            text = binary_data.decode(encoding).strip()
            return text
        except:
            # CP949 디코딩 실패시 다른 인코딩 시도
            try:
                text = binary_data.decode('utf-8', errors='ignore').strip()
                return text
            except:
                return binary_data.hex()
    
    def parse_date(self, date_str, formats=['%Y%m%d', '%Y/%m/%d', '%Y-%m-%d']):
        """날짜 문자열을 파싱하여 datetime 객체 반환"""
        if not date_str or date_str == '00000000' or date_str == '        ':
            return None
            
        for fmt in formats:
            try:
                return datetime.strptime(date_str, fmt).date()
            except:
                continue
        
        return None
    
    def parse_number(self, num_str, is_float=False):
        """숫자 문자열을 파싱"""
        if not num_str or num_str.strip() == '':
            return None
        try:
            if is_float:
                return float(num_str)
            else:
                return int(num_str)
        except:
            return None
    
    def parse(self):
        """파일 파싱 구현 (하위 클래스에서 오버라이드)"""
        raise NotImplementedError("하위 클래스에서 구현해야 합니다.")
    
    def transform(self, parsed_data):
        """데이터 변환 (하위 클래스에서 오버라이드)"""
        raise NotImplementedError("하위 클래스에서 구현해야 합니다.")
    
    def get_data(self):
        """파싱된 데이터 반환"""
        if self.data is None:
            try:
                parsed_data = self.parse()
                self.data = self.transform(parsed_data)
            except Exception as e:
                logger.error(f"{self.market_type} 데이터 파싱 오류: {e}", exc_info=True)
                self.data = pd.DataFrame()  # 빈 DataFrame 반환
                
        return self.data
