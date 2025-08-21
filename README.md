# KIS 종목정보 ETL 파이프라인

한국투자증권(KIS) 마스터 파일을 다운로드하여 데이터베이스에 적재하고 검증하는 ETL 파이프라인입니다.

## 📁 프로젝트 구조

```
├── process_all.py           # 전체 프로세스 실행 (재적재 + 검증)
├── simple_reload.py         # 간단한 재적재 스크립트
├── validation_report.py     # 검증 및 마크다운 리포트 생성
├── full_test.py             # 전체 테스트 (백업→Truncate→재적재→검증)
├── backup_tables.py         # 테이블 백업 (CTAS)
├── check_backup_tables.py   # 백업 테이블 상태 확인
├── reload_data.py           # 상세 재적재 로직
├── db_utils.py              # DB 유틸리티
├── config.py                # DB 및 경로 설정
├── get_files.py             # KIS 마스터 파일 자동 다운로드 (URL 직접 접근)
├── ddl_scripts.sql          # DDL 스크립트
├── requirements.txt         # Python 패키지 의존성
├── parsers/                 # 데이터 파서 클래스들
├── loaders/                 # 데이터 로더 클래스들
├── validation/              # 검증 스크립트들
├── logs/                    # 로그 파일 저장 폴더
├── reports/                 # 검증 리포트 저장 폴더
├── kis_download/            # 원본 데이터 파일 위치
└── bak/                     # 임시/분석 파일 보관 (HTML, 분석 스크립트)
```

## 📝 파일별 역할

| 파일/폴더명 | 역할 | 필요성 |
|-------------|------|--------|
| **process_all.py** | 전체 재적재+검증 실행 메인 | 필수 |
| **simple_reload.py** | 간단 재적재 래퍼 | 필수 |
| **validation_report.py** | 검증 및 마크다운 리포트 생성 | 필수 |
| **full_test.py** | 백업→Truncate→재적재→검증 전체 테스트 | 필수 |
| **backup_tables.py** | CTAS 테이블 백업 유틸리티 | 필수 |
| **check_backup_tables.py** | 백업 테이블 상태 확인 | 필수 |
| **reload_data.py** | 상세 재적재 로직 | 필수 |
| **db_utils.py** | DB 연결/쿼리 유틸리티 | 필수 |
| **config.py** | DB 및 경로 설정 | 필수 |
| **ddl_scripts.sql** | DDL 스크립트 | 필수 |
| **requirements.txt** | Python 패키지 의존성 | 필수 |
| **parsers/** | 데이터 파서 클래스들 | 필수 |
| **loaders/** | 데이터 로더 클래스들 | 필수 |
| **validation/** | 검증 스크립트들 | 필수 |
| **logs/** | 로그 파일 저장 폴더 | 필수 |
| **reports/** | 검증 리포트 저장 폴더 | 필수 |
| **kis_download/** | 원본 데이터 파일 위치 | 필수 |
| **get_files.py** | KIS 마스터 파일 자동 다운로드 (URL 직접 접근) | 선택 |
| **bak/** | 임시/분석 파일 보관 (HTML, 분석 스크립트) | 선택 |

## 🚀 사용법

### 1. 전체 프로세스 실행 (운영 환경)
```bash
python process_all.py
```
- 데이터 재적재 + 검증 + 마크다운 리포트 생성을 한 번에 실행
- 운영 환경에서 정기적으로 실행하는 메인 스크립트

### 2. 전체 테스트 실행 (개발/검증 환경)
```bash
python full_test.py
```
- 백업 → Truncate → 재적재 → 검증을 한 번에 실행
- 개발/테스트 시 데이터 무결성을 보장하면서 전체 파이프라인 검증

### 3. 개별 작업 실행

#### 테이블 백업 (CTAS)
```bash
python backup_tables.py
```

#### 데이터만 재적재
```bash
python simple_reload.py
```

#### 검증 및 리포트만 생성
```bash
python validation_report.py
```

#### 백업 테이블 상태 확인
```bash
python check_backup_tables.py
```

#### 5. 마스터 파일 자동 다운로드
```bash
python get_files.py
```
- KIS API 포털에서 마스터 파일을 자동으로 다운로드
- 다운로드 서버 URL에 직접 접근하여 최신 파일 획득
- `kis_download/` 폴더에 마스터 파일 저장
- 로그: `logs/kis_file_download.log`

## 📊 생성되는 리포트

### 마크다운 리포트
- **경로**: `reports/kis_validation_report_YYYYMMDD_HHMMSS.md`
- **내용**: 검증 결과, 테이블별 통계, 오류 내역

### JSON 리포트
- **경로**: `reports/kis_validation_report_YYYYMMDD_HHMMSS.json`
- **내용**: 구조화된 검증 데이터, API 연동 가능

## 📈 적재되는 데이터

| 테이블명 | 설명 | 예상 건수 |
|----------|------|-----------|
| **instruments** | 주식, ETF, ELW 등 금융상품 정보 | ~4,900건 |
| **sector_code** | 업종코드 정보 | ~485건 |
| **theme_code** | 테마코드 정보 | ~302건 |
| **member_code** | 회원사코드 정보 | ~22건 |

## ✅ 검증 항목

### 필수 검증
- 삼성전자 존재 및 코드 정확성 검증
- 시장별 최소 종목 수 검증 (KOSPI, KOSDAQ 등)
- 참조 테이블 데이터 존재 검증
- 필수 필드 NULL 값 검증

### 통계 검증
- 테이블별 레코드 수 확인
- 시장별 분포 확인
- 업종별 분포 확인

## 📝 로그 파일

| 로그 파일 | 설명 |
|-----------|------|
| `logs/kis_process.log` | 전체 프로세스 로그 |
| `logs/kis_reload_simple.log` | 간단 재적재 로그 |
| `logs/kis_reload.log` | 상세 재적재 로그 |
| `logs/kis_validation.log` | 검증 로그 |
| `logs/kis_backup.log` | 백업 로그 |
| `logs/kis_full_test.log` | 전체 테스트 로그 |
| `logs/kis_file_download.log` | 마스터 파일 다운로드 로그 |

## 🔧 설정

### 데이터베이스 설정
`config.py` 파일에서 데이터베이스 연결 정보를 설정하세요:
```python
DATABASE_CONFIG = {
    'host': 'localhost',
    'port': 3306,
    'user': 'your_user',
    'password': 'your_password',
    'database': 'your_database'
}
```

### 환경 설정
1. Python 3.8 이상 필요
2. 필요한 패키지 설치: `pip install -r requirements.txt`
3. MariaDB/MySQL 데이터베이스 필요

## 🗃️ 백업 및 정리

### 백업 폴더 (`bak/`)
다음 파일들이 보관되어 있습니다:
- **페이지 분석**: `analyze_page_structure.py` - KIS API 포털 구조 분석 도구
- **HTML 파일**: `kis_*.html`, `page_*.html` - 웹페이지 구조 문서
- **구현 문서**: `RELOAD_IMPLEMENTATION.md` - 이전 구현 문서 
- **중복 파일**: `conn_mariadb.py`, `get_files_old.py` - 통합/개선된 기능들
- **파서 백업**: `*_parser_backup.py` - 이전 파서 버전들
- **테스트 파일**: `test_*.py`, `check_*.py` - 개발 중 테스트 스크립트들

### 테이블 백업
- **backup_tables.py**: CTAS 방식으로 테이블 백업
- 백업 테이블명: `{원본테이블명}_backup_{YYYYMMDD_HHMMSS}`
- 백업 상태 확인: `check_backup_tables.py`

## 🔄 워크플로우

### 운영 환경
```bash
# 정기 운영 (매일/매주)
python process_all.py
```
1. **자동 Truncate**: 모든 테이블 초기화
2. **데이터 재적재**: 참조 테이블 → 종목 테이블 순서로 적재
3. **검증 및 리포트**: 자동 검증 후 마크다운 리포트 생성

### 개발/테스트 환경
```bash
# 안전한 전체 테스트 (백업 포함)
python full_test.py
```
1. **자동 백업**: CTAS로 모든 테이블 백업
2. **Truncate**: 테이블 초기화
3. **재적재**: 새 데이터 적재
4. **검증**: 품질 검증 및 리포트 생성

### 개별 작업
```bash
# 1. 백업만
python backup_tables.py

# 2. 재적재만 (Truncate 포함)
python simple_reload.py

# 3. 검증만
python validation_report.py

# 4. 백업 상태 확인
python check_backup_tables.py
```

## 📞 문의사항
프로젝트 관련 문의사항이 있으시면 담당자에게 연락 바랍니다.

## 🏆 프로젝트 상태

### ✅ 완료된 기능
- [x] 자동화된 ETL 파이프라인
- [x] 테이블 백업 시스템 (CTAS)
- [x] 종합 검증 시스템 (100% 통과)
- [x] 마크다운 리포트 자동 생성
- [x] 로그 파일 체계화
- [x] NULL 값 처리 개선 (ELW 특성 반영)
- [x] 프로젝트 구조 정리
- [x] 마스터 파일 자동 다운로드 (URL 직접 접근)

### 📊 최신 검증 결과
- **검증 성공률**: 100% (7/7 항목 통과)
- **적재 데이터**: 4,932개 종목 정상 적재
- **NULL 값 문제**: 완전 해결
- **파일 다운로드**: 15개 마스터 파일 자동 다운로드
- **마지막 업데이트**: 2025-08-21

### 🎯 주요 특징
- **안전한 테스트**: `full_test.py`로 백업 포함 전체 테스트
- **빠른 운영**: `process_all.py`로 일일 배치 처리
- **자동 다운로드**: `get_files.py`로 최신 마스터 파일 자동 획득
- **데이터 품질**: 삼성전자, 시장별 분포 등 핵심 검증
- **자동 리포트**: `reports/` 폴더에 타임스탬프별 리포트 생성
