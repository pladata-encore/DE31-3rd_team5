# DE31-3rd_team5

# 주제
- 전력수급현황 데이터 ETL 파이프라인 구축 및 분석

## 목표
- 수업 시간에 배운 도구를 활용하여, 데이터 수집부터 시각화까지 전체적인 데이터 플로우를 확인하고, 파이프라인 구축을 목표로 함.
- 전력수급 데이터를 효율적으로 수집, 저장, 분석하여 전력기획팀의 중장기적인 전략 수립과 의사결정을 지원
- 전력수급 트렌드를 시간별, 월별, 연도별 분석 및 시각화 대시보드 제작

# 데이터셋
## 데이터셋 정보
- 데이터셋 : 전력통계정보시스템에서 5분 단위로 생성되는 전력수급현황 데이터
https://epsis.kpx.or.kr/epsisnew/selectEkgeEpsMepRealChart.do?menuId=030300
- 수집 기간 : 2021/07/01 ~ ongoing (총 3년)
- 5분 단위 실시간 데이터
- 사용 컬럼
  - 공급능력
  - 현재수요(현재부하)
  - 공급예비력
  - 운영예비력

## 데이터 수집 및 저장
- 데이터 크롤링 주기: 
  하루에 4회 (오전 6시, 오후 12시, 오후 6시, 오전 12시), 전일 데이터는 오전 6시 추가 수집

- 배치성 데이터 수집 이유
  - 자원 효율성: 
  일정한 주기로 데이터를 수집함으로써 시스템 자원을 효율적으로 관리 및 서버 부담을 최소화
  - 정기적 분석: 
  일일 단위의 데이터를 집계하여 전력수급의 장기적인 트렌드를 분석하며, 실시간 데이터보다 더 명확한   패턴을 발견 가능성 증가
  - 데이터 관리 용이성: 
  일별 데이터를 폴더별로 저장하여 관리함으로써 데이터 접근성 제고 및 장기적인 데이터 관리 용이

- 데이터 저장: 
HDFS에 일자별로 저장하여 관리


# 사용 기술 스택
![image](https://github.com/pladata-encore/DE31-3rd_team6/assets/155427737/1eaa9172-27ca-4830-9765-200a482b66f6)


# 구현 과정

## 1) 전력 데이터 크롤링
- 데이터셋 확인 및 필요한 데이터 항목 결정
- 수집 기간 내 데이터 크롤링 및 저장

## 2) airflow - 실시간 전력 데이터 수집 및 저장

### 플로우 설계
1. 당일 날짜 데이터를 만드는 PythonOperator 작성
    1. 배치성으로 관리
        1. 데이터를 수집하는 목적 및 범위를 축소 시켜, 배치성으로 관리할 예정
        2. 내부 보고를 목적으로 함
        3. 대시보드 연동
    2. 당일 데이터 - 하루에 4번 수집 (오전 6시, 오후 12시, 오후6시, 오전 12시)
    3. 전일 데이터 - 하루 1번, 오전 6시에 한 번 크롤링( 데이터 크롤링 시, 웹페이지 상 데이터가 10~15분 지연되는 경우가 있음. 데이터를 호출하는 게 일자별 호출이기 때문에, 전일 데이터를 하루에 한 번 다시 업데이트 하는 것으로 해결)

2. [전력통계정보시스템](https://epsis.kpx.or.kr/epsisnew/selectEkgeEpsMepRealChart.do?menuId=030300&locale=) 으로부터 웹 크롤링하는 pythonOperator 작성 (Extract)

3. Preprocessing 
    1. Date/time 분리하는 컬럼 생성하는 데이터 작성(Extract)
    2. missing value 확인
    3. 공급 대비 수요량 퍼센테이지 계산하기 (Translate)
    4. 파생변수 컬럼 생성

4. 데이터 저장하는 pythonOperator 작성 - 원본데이터 저장
    1. hadoop 서버에 csv 파일로 저장
  
### DAG 작성
- real_time_task >> yesterday_task
  - real_time_task : 현재 날짜 기준 실시간 전력 데이터 6시간마다 로드 및 기존 DF에 append → 해당 날짜를 파일명으로 하는 csv 에 저장 → HDFS에 csv 파일로 저장
  - yesterday_task : 전일 23:45 ~ 데이터 안 들어왔을 경우를 대비, 전일 데이터 전체 덮어쓰기 → 해당 날짜를 파일명으로 하는 csv 에 저장 → HDFS에 csv 파일로 저장


## 3) Hadoop 및 Spark 클러스터 구축
- Hadoop 클러스터(server1, server2, server3) 구성 및 설정
- Spark 클러스터 구성하여 데이터 처리 및 분석 환경 구축
![image](https://github.com/pladata-encore/DE31-3rd_team6/assets/155427737/8c48dc72-a418-4b6f-8939-656967c4f8f9)

## 4) 데이터 분석 및 시각화

### PySpark를 활용한 데이터 처리 및 분석 수행

![image](https://github.com/pladata-encore/DE31-3rd_team6/assets/155427737/8543db47-27df-4bad-942d-847b67a2b9bf)


### Tableau, Power BI를 이용한 데이터 시각화 대시보드 제작
  - 태블로 링크 : https://public.tableau.com/app/profile/hyeonji.kim7672/viz/mini3_17200759216340/1?publish=yes
![image](https://github.com/pladata-encore/DE31-3rd_team6/assets/155427737/f7aaec71-31cb-4425-865d-5a11c428fc79)

![image](https://github.com/pladata-encore/DE31-3rd_team6/assets/155427737/64c4dc41-9e80-4f21-ac45-56a243fe60da)


