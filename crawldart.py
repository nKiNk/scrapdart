import pandas as pd
from bs4 import BeautifulSoup
import requests
import json
import pprint
import lxml.html
import re
from pprint import pprint
import logging 

logger = logging.getLogger(__name__)
# logging.basicConfig(filename='./log/debug.log', level=logging.DEBUG)
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter('[%(asctime)s][%(levelname)s|%(filename)s:%(lineno)s] >> %(message)s')

streamHandler = logging.StreamHandler()
fileHandler = logging.FileHandler('./log/debug.log')

streamHandler.setFormatter(formatter)
fileHandler.setFormatter(formatter)

logger.addHandler(streamHandler)
logger.addHandler(fileHandler)

base_url = 'http://dart.fss.or.kr/api/search.json?auth='
search_url = 'http://dart.fss.or.kr/dsae001/search.ax?textCrpNm='
detail_url = 'http://dart.fss.or.kr/dsae001/selectPopup.ax?selectKey=' 
auth_key =  '##########################' # enter your auth_key registered at https://dart.fss.or.kr/dsap001/apikeyManagement.do
start_dt = '20050101' # start date(YYYYMMDD) 2004년 이전에는 sub menu의 양식이 다른 경우가 있음
page_set = '100' # 1 page당 출력 리스트


# 회사명으로 회사코드 반환
def getCrpCd(crpNm):
  try:
    crpurl = search_url+crpNm
  except:
    print("회사명을 올바른 형식으로 입력해주십시오.")
  response = requests.get(crpurl)
  soup = BeautifulSoup(response.text, "html.parser")
  crpList = []
  crpList.append(soup.find("table").findAll("a"))
  crpCdDict = {}
  
  for eachCrp in crpList[0]:
    crpCd = eachCrp['href'].split("selectKey=")[1] # .split('"')[0]
    crpFullNm = eachCrp.text
    crpCdDict[crpCd]=crpFullNm
  return crpCdDict

# 회사코드로 회사 개황 반환
def getCrpDetail(crpCd):
  try:
    dturl = detail_url + crpCd
  except:
    print("회사명을 올바른 형식으로 입력해주십시오.")
  response = requests.get(dturl)
  soup = BeautifulSoup(response.text, "html.parser")
  crpDtlDict = {}
  for eachLine in soup.find("tbody").findAll("tr"):
    dtlNm = eachLine.th.text
    dtlContent = eachLine.td.get_text(strip=True)
    crpDtlDict[dtlNm]=dtlContent
  return crpDtlDict

#회사의 특정 기간 동안의 서브 메뉴상 테이블을 모두 취합
# crp_cd = '*********' # company code(6 digit for listed companies and 8 digit for private companies which can be found at URL of "기업개황정보")
# bsn_tp = 'D001' # https://dart.fss.or.kr/dsap001/guide.do "상세유형" 참조
# sub_menu = '2. 세부변동내역' # change "2. 세부변동내역" to specific submenu


def crawldart(crp_cd,bsn_tp,sub_menu):
    url = f'{base_url}{auth_key}&start_dt={start_dt}&crp_cd={crp_cd}&bsn_tp={bsn_tp}&page_set={page_set}'
    print(url)
    response = requests.get(url)
    data = json.loads(response.content)
    total_page = data['total_page']
    page_list = data['list']

    i = 1
    while i < total_page:
        i += 1
        response = requests.get(url + '&page_no='+str(i))
        data = json.loads(response.content)
        page_list = page_list + data['list']

    
    i = 0
    j = len(page_list)
    df_all = pd.DataFrame()
    columnlist = ['성명(명칭)', '생년월일 또는 사업자등록번호 등', '변동일', '취득/처분방법',
                  '주식등의 종류', '변동전', '증감', '변동후', '취득/처분 단가', '비고'] # change columns as you change sub menu
    
    for each_doc in page_list:
        i += 1
        print(f"processing {i}/{j}")
        html_url = 'http://dart.fss.or.kr/dsaf001/main.do?rcpNo='
        each_url = f"{html_url}{each_doc['rcp_no']}"
        req = requests.get(each_url)
        try:
          dcm_body = req.text.split(f'text: "{sub_menu}"')[1].split("viewDoc(")[1].split(")")[0] 
          dcm_body_list = dcm_body.replace("'", "").replace(" ", "").split(",")
          each_doc['dcmNo'] = dcm_body_list[1]
          each_doc['eleId'] = dcm_body_list[2]
          each_doc['offset'] = dcm_body_list[3]
          each_doc['length'] = dcm_body_list[4]
          each_doc['dtd'] = dcm_body_list[5]
        except:
          logger.debug(str(i) + "_" + each_url)

        html_url = 'http://dart.fss.or.kr/report/viewer.do?rcpNo={0}&dcmNo={1}&eleId={2}&offset={3}&length={4}&dtd={5}'
        print("crawling")    
        each_url = html_url.format(each_doc['rcp_no'], each_doc['dcmNo'],each_doc['eleId'], each_doc['offset'], each_doc['length'], each_doc['dtd'])
        req = requests.get(each_url)
        soup = BeautifulSoup(req.text, "html.parser")
        try:
          table = soup.find("table")
          df = pd.read_html(str(table))[0]
          df.columns = columnlist
          df.insert(0, 'RMK', each_doc['rmk'])
          df.insert(0, '보고서명', each_doc['rpt_nm'])
          df.insert(0, '공시대상회사', each_doc['crp_nm'])
          df.insert(0, '제출인', each_doc['flr_nm'])
          df.insert(0, '접수일자', each_doc['rcp_dt']) #접수일을 column에 추가
          df_all = pd.concat([df_all, df])
        except:
          logger.debug(str(i) + " " + each_url)
        

    df_all.reset_index(drop=True)
    df_all.to_csv(f'./output/{crp_cd}_{bsn_tp}_{sub_menu}.csv') #data.csv로 저장
