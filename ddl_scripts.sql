-- KIS 종목정보 테이블 DDL 스크립트

-- 1. 통합 종목 정보 테이블 (instruments)
CREATE TABLE IF NOT EXISTS instruments (
    -- 기본 식별 정보
    instrument_code VARCHAR(20) NOT NULL COMMENT '종목코드',
    short_code VARCHAR(12) COMMENT '단축코드',
    instrument_name VARCHAR(150) NOT NULL COMMENT '종목명',
    instrument_name_eng VARCHAR(150) COMMENT '영문종목명',
    alias_names VARCHAR(4000) COMMENT '별칭/대체이름(여러 별칭을 구분자로 연결)',
    
    -- 종목 분류 정보
    instrument_type VARCHAR(20) NOT NULL COMMENT '상품유형(STOCK/FUTURE/OPTION/ELW/BOND/ETF/INDEX 등)',
    instrument_subtype VARCHAR(30) COMMENT '상품세부유형(INDEX_FUTURE/STOCK_OPTION/NIGHT_FUTURE 등)',
    market_type VARCHAR(20) NOT NULL COMMENT '시장구분(KOSPI/KOSDAQ/KONEX/NYSE/NASDAQ/SSE 등)',
    country_code VARCHAR(3) COMMENT '국가코드(KOR/USA/JPN 등)',
    currency_code VARCHAR(3) COMMENT '통화코드(KRW/USD/JPY 등)',
    exchange_code VARCHAR(10) COMMENT '거래소코드',
    
    -- 공통 일자 정보
    listing_date DATE COMMENT '상장일',
    maturity_date DATE COMMENT '만기일(파생/채권)',
    last_trading_date DATE COMMENT '최종거래일(파생)',
    issue_date DATE COMMENT '발행일(채권/ELW 등)',
    
    -- 가격/거래 관련 정보
    face_value DECIMAL(15,2) COMMENT '액면가',
    price_tick DECIMAL(15,4) COMMENT '가격틱사이즈(파생)',
    tick_value DECIMAL(15,2) COMMENT '틱가치(파생)',
    price_decimal INT DEFAULT 0 COMMENT '가격소수점자리수',
    trading_unit INT COMMENT '매매단위',
    trading_hours VARCHAR(100) COMMENT '거래시간',
    
    -- 발행 관련 정보
    issue_cnt BIGINT COMMENT '상장주식수(주식)',
    market_cap BIGINT COMMENT '시가총액(주식)',
    issuer_code VARCHAR(10) COMMENT '발행기관코드',
    issuer_name VARCHAR(100) COMMENT '발행기관명',
    
    -- 파생상품 관련 정보
    underlying_asset VARCHAR(100) COMMENT '기초자산명(파생/ELW)',
    underlying_code VARCHAR(20) COMMENT '기초자산코드(파생/ELW)',
    exercise_type VARCHAR(10) COMMENT '권리유형(콜/풋)',
    strike_price DECIMAL(15,2) COMMENT '행사가격(파생/ELW)',
    contract_size INT COMMENT '계약크기/승수(파생)',
    settlement_method VARCHAR(20) COMMENT '결제방법(현물/현금)',
    parity_ratio DECIMAL(15,6) COMMENT '전환비율(ELW)',
    
    -- 채권 관련 정보
    interest_rate DECIMAL(8,4) COMMENT '표면금리(채권)',
    interest_payment_type VARCHAR(10) COMMENT '이자지급방법(채권)',
    credit_rating VARCHAR(10) COMMENT '신용등급(채권)',
    
    -- 분류 코드
    industry_code VARCHAR(10) COMMENT '업종/섹터코드',
    
    -- 상태 플래그
    is_etf CHAR(1) DEFAULT 'N' COMMENT 'ETF여부(Y/N)',
    is_etn CHAR(1) DEFAULT 'N' COMMENT 'ETN여부(Y/N)',
    is_elw CHAR(1) DEFAULT 'N' COMMENT 'ELW여부(Y/N)',
    is_reit CHAR(1) DEFAULT 'N' COMMENT 'REIT여부(Y/N)',
    is_spac CHAR(1) DEFAULT 'N' COMMENT 'SPAC여부(Y/N)',
    is_foreign CHAR(1) DEFAULT 'N' COMMENT '외국주권여부(Y/N)',
    is_dr CHAR(1) DEFAULT 'N' COMMENT 'DR여부(Y/N)',
    is_index CHAR(1) DEFAULT 'N' COMMENT '지수여부(Y/N)',
    is_warning CHAR(1) DEFAULT 'N' COMMENT '투자경고종목여부(Y/N)',
    is_risk CHAR(1) DEFAULT 'N' COMMENT '위험종목여부(Y/N)',
    is_caution CHAR(1) DEFAULT 'N' COMMENT '주의종목여부(Y/N)',
    is_managed CHAR(1) DEFAULT 'N' COMMENT '관리종목여부(Y/N)',
    is_credit_available CHAR(1) DEFAULT 'N' COMMENT '신용주문가능여부(Y/N)',
    is_basket CHAR(1) DEFAULT 'N' COMMENT '바스켓구성여부(ELW)',
    is_night_trade CHAR(1) DEFAULT 'N' COMMENT '야간거래여부(Y/N)',
    
    -- 추가 정보 (JSON 형식으로 저장되는 타입별 특수 정보)
    extra_data JSON COMMENT '상품유형별 추가정보(JSON)',
    
    -- 관리 정보
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '생성일시',
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '수정일시',
    
    PRIMARY KEY (instrument_code),
    INDEX idx_short_code (short_code),
    INDEX idx_type_market (instrument_type, market_type),
    INDEX idx_underlying (underlying_code),
    INDEX idx_country_market (country_code, market_type),
    FULLTEXT INDEX ft_alias (alias_names),
    FULLTEXT INDEX ft_name (instrument_name, instrument_name_eng)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='통합 종목정보';

-- 2. 회원사 코드 테이블 (member_code)
CREATE TABLE IF NOT EXISTS member_code (
    member_code VARCHAR(5) NOT NULL COMMENT '회원사코드',
    member_name VARCHAR(100) NOT NULL COMMENT '회원사명',
    member_abbr VARCHAR(20) COMMENT '회원사약어명',
    member_eng_name VARCHAR(100) COMMENT '회원사영문명',
    member_type VARCHAR(20) COMMENT '회원사구분',
    reg_date DATE COMMENT '등록일',
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '생성일시',
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '수정일시',
    PRIMARY KEY (member_code)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='회원사코드 정보';

-- 3. 업종 코드 테이블 (sector_code)
CREATE TABLE IF NOT EXISTS sector_code (
    sector_code VARCHAR(10) NOT NULL COMMENT '업종코드',
    sector_name VARCHAR(100) NOT NULL COMMENT '업종명',
    market_type VARCHAR(10) COMMENT '시장구분',
    sector_type VARCHAR(20) COMMENT '업종구분(대/중/소)',
    parent_code VARCHAR(10) COMMENT '상위업종코드',
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '생성일시',
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '수정일시',
    PRIMARY KEY (sector_code),
    INDEX idx_parent (parent_code)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='업종코드 정보';

-- 4. 테마 코드 테이블 (theme_code)
CREATE TABLE IF NOT EXISTS theme_code (
    theme_code VARCHAR(10) NOT NULL COMMENT '테마코드',
    theme_name VARCHAR(100) NOT NULL COMMENT '테마명',
    theme_group VARCHAR(50) COMMENT '테마그룹',
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '생성일시',
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '수정일시',
    PRIMARY KEY (theme_code),
    INDEX idx_theme_group (theme_group)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='테마코드 정보';

-- 5. 테마-종목 매핑 테이블 (instrument_theme_map)
CREATE TABLE IF NOT EXISTS instrument_theme_map (
    instrument_code VARCHAR(20) NOT NULL COMMENT '종목코드',
    theme_code VARCHAR(10) NOT NULL COMMENT '테마코드',
    weight DECIMAL(5,2) COMMENT '테마 내 비중(%)',
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '생성일시',
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '수정일시',
    PRIMARY KEY (instrument_code, theme_code),
    INDEX idx_theme (theme_code),
    CONSTRAINT fk_theme_map_instrument FOREIGN KEY (instrument_code) REFERENCES instruments (instrument_code) ON DELETE CASCADE,
    CONSTRAINT fk_theme_map_theme FOREIGN KEY (theme_code) REFERENCES theme_code (theme_code) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='종목-테마 매핑 정보';
