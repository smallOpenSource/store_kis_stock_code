import os
import zipfile
import logging
import pandas as pd
import sys

# 상위 디렉토리를 Python 경로에 추가
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import DATA_DIR

logger = logging.getLogger('file_analyzer')

class FileAnalyzer:
    """원시 파일 분석기"""
    
    def __init__(self):
        self.file_stats = {}
    
    def analyze_master_file(self, file_path, record_size=None):
        """마스터 파일 분석 (라인 수, 레코드 수 등)"""
        try:
            full_path = os.path.join(DATA_DIR, file_path) if not os.path.isabs(file_path) else file_path
            
            # ZIP 파일인 경우 압축 해제 후 분석
            if full_path.endswith('.zip'):
                with zipfile.ZipFile(full_path, 'r') as zip_ref:
                    file_list = zip_ref.namelist()
                    master_file = None
                    for file in file_list:
                        if file.endswith(('.mst', '.cod')):
                            master_file = file
                            break
                    
                    if master_file:
                        with zip_ref.open(master_file) as f:
                            content = f.read()
                            return self._analyze_binary_content(content, record_size, master_file)
                    else:
                        logger.warning(f"ZIP 파일에서 마스터 파일을 찾을 수 없음: {full_path}")
                        return None
            else:
                # 일반 파일인 경우
                if os.path.exists(full_path):
                    with open(full_path, 'rb') as f:
                        content = f.read()
                        return self._analyze_binary_content(content, record_size, os.path.basename(full_path))
                else:
                    logger.warning(f"파일이 존재하지 않음: {full_path}")
                    return None
                    
        except Exception as e:
            logger.error(f"파일 분석 오류 {file_path}: {e}")
            return None
    
    def _analyze_binary_content(self, content, record_size, filename):
        """바이너리 내용 분석"""
        total_size = len(content)
        
        if record_size and record_size > 0:
            # 고정 길이 레코드인 경우
            record_count = total_size // record_size
            remainder = total_size % record_size
        else:
            # 가변 길이인 경우 (라인 수 계산)
            try:
                text_content = content.decode('cp949', errors='ignore')
                record_count = text_content.count('\n')
                if not text_content.endswith('\n') and text_content:
                    record_count += 1  # 마지막 라인에 개행문자가 없는 경우
            except:
                try:
                    text_content = content.decode('utf-8', errors='ignore')
                    record_count = text_content.count('\n')
                    if not text_content.endswith('\n') and text_content:
                        record_count += 1
                except:
                    record_count = None
            remainder = 0
        
        return {
            'filename': filename,
            'total_size': total_size,
            'record_count': record_count,
            'remainder': remainder,
            'record_size': record_size
        }
    
    def analyze_all_files(self, file_mapping):
        """모든 마스터 파일 분석"""
        results = {}
        
        # 각 종목 유형별 레코드 크기 정의 (바이트)
        record_sizes = {
            '코스피': 312,
            '코스닥': 312,
            '코넥스': 312,
            'ELW': 300,
            '지수선물옵션': 200,
            '주식선물옵션': 200,
            '상품선물옵션': 200,
            '회원사코드': 50,
            '업종코드': None,  # 가변 길이
            '테마코드': None,  # 가변 길이
            '채권코드': 350,
        }
        
        logger.info("파일 분석 시작...")
        
        for _, row in file_mapping.iterrows():
            item_name = row['항목명']
            master_file = row['종목다운로드']
            
            if pd.isna(master_file) or master_file.strip() == '':
                logger.warning(f"{item_name}: 마스터 파일명이 없습니다.")
                continue
            
            record_size = record_sizes.get(item_name)
            stats = self.analyze_master_file(master_file, record_size)
            
            if stats:
                results[item_name] = stats
                logger.info(f"{item_name}: {stats['record_count']:,}개 레코드 (파일크기: {stats['total_size']:,}bytes)")
                if stats['remainder'] > 0:
                    logger.warning(f"{item_name}: 나머지 바이트 {stats['remainder']}bytes 있음")
            else:
                logger.error(f"{item_name}: 파일 분석 실패")
        
        self.file_stats = results
        return results
