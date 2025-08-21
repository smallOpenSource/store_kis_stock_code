#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import requests
import csv
import logging
import time
from urllib.parse import unquote
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from bs4 import BeautifulSoup

# 로깅 설정
os.makedirs('logs', exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/kis_file_download.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def safe_name(name):
    """윈도우 파일명에서 특수문자 제거/변환"""
    return "".join(c if c not in r'\/:*?"<>|' else '_' for c in name)

def fetch_kis_api_page_with_selenium():
    """Selenium을 사용하여 KIS API 포털에서 테이블 정보 추출"""
    logger.info("Selenium으로 KIS API 포털 접근 중...")
    
    options = Options()
    options.add_argument('--headless')  # 헤드리스 모드
    options.add_argument('--disable-gpu')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--window-size=1920,1080')
    options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36')
    
    driver = None
    try:
        # WebDriver 초기화
        driver = webdriver.Chrome(options=options)
        
        # 대상 URL 목록
        urls = [
            "https://apiportal.koreainvestment.com/apiservice-category",
            "https://apiportal.koreainvestment.com/masterfile",
            "https://apiportal.koreainvestment.com/download"
        ]
        
        file_info_list = []
        
        for url in urls:
            logger.info(f"페이지 로딩 중: {url}")
            driver.get(url)
            
            # 페이지가 완전히 로드될 때까지 대기 (최대 20초)
            try:
                WebDriverWait(driver, 20).until(
                    EC.presence_of_element_located((By.TAG_NAME, "table"))
                )
                logger.info(f"페이지 로드 완료: {url}")
            except TimeoutException:
                logger.warning(f"테이블을 찾을 수 없음: {url}")
                continue
            
            # 테이블 찾기
            tables = driver.find_elements(By.TAG_NAME, "table")
            logger.info(f"{len(tables)}개 테이블 발견")
            
            if not tables:
                logger.warning(f"테이블을 찾을 수 없음: {url}")
                continue
            
            # 테이블 내용 추출
            for table_idx, table in enumerate(tables):
                logger.info(f"테이블 {table_idx+1} 분석 중...")
                
                rows = table.find_elements(By.TAG_NAME, "tr")
                
                for row in rows:
                    try:
                        tds = row.find_elements(By.TAG_NAME, "td")
                        if len(tds) < 2:
                            continue
                        
                        # 다운로드 링크 추출 (첫 번째 열)
                        download_cell = tds[0]
                        download_link = download_cell.find_elements(By.TAG_NAME, "a")
                        
                        if not download_link:
                            continue
                            
                        download_link = download_link[0]
                        kor_name = safe_name(download_link.text.strip())
                        url_download = download_link.get_attribute("href")
                        
                        if not url_download or url_download == '#':
                            continue
                        
                        # 헤더/샘플 링크 추출 (두 번째 열)
                        url_header = ''
                        url_sample = ''
                        
                        links_cell = tds[1]
                        links = links_cell.find_elements(By.TAG_NAME, "a")
                        
                        for link in links:
                            link_text = link.text.strip()
                            href = link.get_attribute("href")
                            
                            if link_text == '헤더정보':
                                url_header = href
                            elif link_text == '샘플코드':
                                url_sample = href
                        
                        # 유효한 결과만 추가
                        if kor_name and url_download:
                            file_info = {
                                'name': kor_name,
                                'download_url': url_download,
                                'header_url': url_header,
                                'sample_url': url_sample
                            }
                            
                            # 중복 제거
                            if file_info not in file_info_list:
                                file_info_list.append(file_info)
                                logger.info(f"항목 추가: {kor_name}")
                    
                    except Exception as e:
                        logger.warning(f"행 처리 중 오류: {e}")
                        continue
            
            # 다음 URL로 이동하기 전에 잠시 대기
            time.sleep(2)
        
        logger.info(f"총 {len(file_info_list)}개 파일 정보 추출 완료")
        return file_info_list
        
    except Exception as e:
        logger.error(f"Selenium 처리 중 오류: {e}")
        # 실패하면 정적 URL 목록 사용
        return get_master_file_urls()
    
    finally:
        # WebDriver 종료
        if driver:
            driver.quit()

def get_master_file_urls():
    """정적 마스터 파일 URL 목록 (셀레니움 실패 시 백업)"""
    # 기존 코드 유지
    base_url = "https://new.real.download.dws.co.kr/common/master/"
    
    master_files = [
        # 국내주식
        {"name": "코스피", "file": "kospi_code.mst.zip"},
        {"name": "코스닥", "file": "kosdaq_code.mst.zip"},
        {"name": "코넥스", "file": "konex_code.mst.zip"},
        {"name": "ELW", "file": "elw_code.mst.zip"},
        
        # 파생상품
        {"name": "지수선물옵션", "file": "fo_idx_code_mts.mst.zip"},
        {"name": "주식선물옵션", "file": "fo_stk_code_mts.mst.zip"},
        {"name": "상품선물옵션", "file": "fo_com_code.mst.zip"},
        
        # 야간파생
        {"name": "KRX연계 야간옵션", "file": "fo_eurex_code.mst.zip"},
        {"name": "KOSPI200 야간선물", "file": "fo_cme_code.mst.zip"},
        {"name": "미국달러 야간선물", "file": "fo_cmu_code.mst.zip"},
        
        # 기타
        {"name": "업종코드", "file": "idxcode.mst.zip"},
        {"name": "테마코드", "file": "theme_code.mst.zip"},
        {"name": "채권코드", "file": "bond_code.mst.zip"},
        
        # 해외종목 (나스닥 예시)
        {"name": "미국(나스닥)", "file": "nasmst.cod.zip"},
    ]
    
    # 회원사코드는 zip이 아님
    master_files.append({"name": "회원사코드", "file": "memcode.mst"})
    
    # 전체 URL 구성 - 마스터 파일만
    result = []
    for item in master_files:
        download_url = base_url + item["file"]
        
        result.append({
            'name': item["name"],
            'download_url': download_url,
            'header_url': '',
            'sample_url': ''
        })
    
    return result

def download_files(file_info_list, download_folder):
    """파일 다운로드 (마스터, 헤더, 샘플)"""
    # 기존 코드 유지
    os.makedirs(download_folder, exist_ok=True)
    
    results = []
    total_files = len(file_info_list)
    
    for idx, info in enumerate(file_info_list, 1):
        logger.info(f"[{idx}/{total_files}] {info['name']} 다운로드 중...")
        
        files = []
        
        # 1. 마스터 파일 다운로드
        fn_dl = download_single_file(
            info['download_url'], 
            f"{info['name']}_마스터_", 
            download_folder
        )
        files.append(fn_dl)
        
        # 2. 헤더정보 다운로드 (GitHub)
        fn_header = ''
        if info['header_url'] and info['header_url'] != "#" and info['header_url'] != "":
            fn_header = download_single_file(
                info['header_url'],
                f"{info['name']}_헤더_",
                download_folder
            )
        files.append(fn_header)
        
        # 3. 샘플코드 다운로드 (GitHub)
        fn_sample = ''
        if info['sample_url'] and info['sample_url'] != "#" and info['sample_url'] != "":
            fn_sample = download_single_file(
                info['sample_url'],
                f"{info['name']}_샘플_",
                download_folder
            )
        files.append(fn_sample)
        
        results.append([info['name']] + files)
        
        # 요청 간 간격
        time.sleep(0.5)
    
    return results

def download_single_file(url, prefix, folder):
    """단일 파일 다운로드 (마스터 파일 또는 GitHub raw 파일)"""
    # 기존 코드 유지
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        
        # GitHub URL인 경우 /blob/을 /raw/로 변환
        if 'github.com' in url and '/blob/' in url:
            url = url.replace('/blob/', '/raw/')
        
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()
        
        # 파일명 결정
        filename = safe_name(unquote(os.path.basename(url)))
        if not filename or filename == '' or filename == '/':
            # Content-Disposition 헤더에서 파일명 추출 시도
            content_disposition = response.headers.get('content-disposition', '')
            if 'filename=' in content_disposition:
                filename = content_disposition.split('filename=')[1].strip('"\'')
            else:
                # GitHub 파일인 경우 URL에서 파일명 추출
                if 'github.com' in url:
                    url_parts = url.split('/')
                    filename = url_parts[-1] if url_parts else 'unknown_file'
                else:
                    filename = 'unknown_file'
        
        full_filename = f"{prefix}{filename}"
        file_path = os.path.join(folder, full_filename)
        
        with open(file_path, "wb") as f:
            f.write(response.content)
        
        logger.info(f"  ✓ 다운로드 성공: {full_filename}")
        return full_filename
        
    except Exception as e:
        logger.error(f"  ✗ 다운로드 실패 ({url}): {e}")
        return ''

def save_csv_report(results, csv_file):
    """CSV 리포트 저장"""
    # 기존 코드 유지
    try:
        # 기존 CSV 파일이 있으면 백업
        if os.path.exists(csv_file):
            backup_file = csv_file.replace('.csv', '_backup.csv')
            if os.path.exists(backup_file):
                os.remove(backup_file)
            os.rename(csv_file, backup_file)
        
        with open(csv_file, 'w', newline='', encoding='utf-8-sig') as f:
            writer = csv.writer(f)
            writer.writerow(['항목명', '마스터파일', '헤더정보', '샘플코드'])
            writer.writerows(results)
        
        logger.info(f"CSV 리포트 저장: {csv_file}")
        
    except Exception as e:
        logger.warning(f"CSV 저장 실패: {e}")
        # CSV 저장에 실패해도 전체 프로세스는 성공으로 처리
        logger.info("CSV 저장은 실패했지만 마스터 파일 다운로드는 성공")

def get_master_file_urls_from_html():
    """마스터 파일 목록을 KIS 테이블 HTML에서 추출"""
    logger.info("KIS HTML 파일을 통한 링크 추출 시도...")
    
    file_info_list = []
    
    try:
        # KIS HTML 파일 읽기 (없으면 백업 사용)
        html_path = 'kis_table.html'
        if not os.path.exists(html_path):
            logger.warning(f"{html_path} 파일이 없습니다. 백업 URL 사용.")
            return get_static_backup_urls()
        
        with open(html_path, 'r', encoding='utf-8') as f:
            html = f.read()
        
        soup = BeautifulSoup(html, 'html.parser')
        tables = soup.select('table.data-table')
        
        if not tables:
            logger.warning("HTML에서 테이블을 찾을 수 없습니다. 백업 URL 사용.")
            return get_static_backup_urls()
        
        for table in tables:
            rows = table.find_all('tr')
            
            for row in rows:
                cells = row.find_all('td')
                if len(cells) < 2:
                    continue
                
                # 첫 번째 셀: 다운로드 링크
                download_link = cells[0].find('a')
                if not download_link:
                    continue
                
                kor_name = safe_name(download_link.text.strip())
                url_download = download_link.get('href', '')
                
                if not url_download or url_download == '#':
                    continue
                
                # 두 번째 셀: 헤더/샘플 링크
                url_header = ''
                url_sample = ''
                
                links = cells[1].find_all('a')
                for link in links:
                    link_text = link.text.strip()
                    href = link.get('href', '')
                    
                    if link_text == '헤더정보':
                        url_header = href
                    elif link_text == '샘플코드':
                        url_sample = href
                
                # 결과 추가
                file_info = {
                    'name': kor_name,
                    'download_url': url_download,
                    'header_url': url_header,
                    'sample_url': url_sample
                }
                
                file_info_list.append(file_info)
        
        logger.info(f"HTML에서 {len(file_info_list)}개 파일 정보 추출 완료")
        return file_info_list
        
    except Exception as e:
        logger.error(f"HTML 파싱 오류: {e}")
        return get_static_backup_urls()

def get_static_backup_urls():
    """정적 백업 URL (모든 방법 실패 시 마지막 수단)"""
    logger.warning("정적 백업 URL 목록 사용")
    # 기존 하드코딩된 URL 목록은 백업으로만 사용
    base_url = "https://new.real.download.dws.co.kr/common/master/"
    
    master_files = [
        # 국내주식
        {"name": "코스피", "file": "kospi_code.mst.zip"},
        {"name": "코스닥", "file": "kosdaq_code.mst.zip"},
        {"name": "코넥스", "file": "konex_code.mst.zip"},
        {"name": "ELW", "file": "elw_code.mst.zip"},
        
        # 파생상품
        {"name": "지수선물옵션", "file": "fo_idx_code_mts.mst.zip"},
        {"name": "주식선물옵션", "file": "fo_stk_code_mts.mst.zip"},
        {"name": "상품선물옵션", "file": "fo_com_code.mst.zip"},
        
        # 야간파생
        {"name": "KRX연계 야간옵션", "file": "fo_eurex_code.mst.zip"},
        {"name": "KOSPI200 야간선물", "file": "fo_cme_code.mst.zip"},
        {"name": "미국달러 야간선물", "file": "fo_cmu_code.mst.zip"},
        
        # 기타
        {"name": "업종코드", "file": "idxcode.mst.zip"},
        {"name": "테마코드", "file": "theme_code.mst.zip"},
        {"name": "채권코드", "file": "bond_code.mst.zip"},
        
        # 해외종목 (나스닥 예시)
        {"name": "미국(나스닥)", "file": "nasmst.cod.zip"},
    ]
    
    # 회원사코드는 zip이 아님
    master_files.append({"name": "회원사코드", "file": "memcode.mst"})
    
    # 전체 URL 구성 - 마스터 파일만
    result = []
    for item in master_files:
        download_url = base_url + item["file"]
        
        result.append({
            'name': item["name"],
            'download_url': download_url,
            'header_url': '',
            'sample_url': ''
        })
    
    return result

def main():
    """메인 실행 함수"""
    try:
        logger.info("=== KIS 마스터 파일 + 헤더정보 + 샘플코드 자동 다운로드 시작 ===")
        
        # 1. Selenium으로 KIS API 포털에서 파일 정보 가져오기
        file_info_list = fetch_kis_api_page_with_selenium()
        
        if not file_info_list:
            logger.error("다운로드할 파일 정보를 찾을 수 없습니다.")
            return False
        
        # 2. 파일 다운로드 (마스터 + 헤더 + 샘플)
        download_folder = 'kis_download'
        results = download_files(file_info_list, download_folder)
        
        # 3. CSV 리포트 저장 (실패해도 전체 프로세스는 계속)
        csv_file = os.path.join(download_folder, 'kis_file_sets.csv')
        save_csv_report(results, csv_file)
        
        # 4. 다운로드 결과 요약
        total_files = len(results)
        successful_masters = sum(1 for r in results if r[1])  # 마스터파일
        successful_headers = sum(1 for r in results if r[2])  # 헤더정보
        successful_samples = sum(1 for r in results if r[3])  # 샘플코드
        
        logger.info("=== KIS 파일 자동 다운로드 완료 ===")
        logger.info(f"전체 항목: {total_files}개")
        logger.info(f"마스터파일: {successful_masters}/{total_files}개 성공")
        logger.info(f"헤더정보: {successful_headers}/{total_files}개 성공")
        logger.info(f"샘플코드: {successful_samples}/{total_files}개 성공")
        logger.info(f"저장 위치: {download_folder}")
        
        return True
        
    except Exception as e:
        logger.error(f"전체 프로세스 실패: {e}")
        return False

if __name__ == "__main__":
    success = main()
    if success:
        print("✅ KIS 마스터파일 + 헤더정보 + 샘플코드 다운로드 성공!")
    else:
        print("❌ KIS 파일 다운로드 실패!")
