import pandas as pd
from bs4 import BeautifulSoup
import requests
import json
import pprint
import lxml.html
import re
from pprint import pprint

base_url = 'http://dart.fss.or.kr/api/search.json?auth='
auth_key = '******************************' # enter your auth_key registered at https://dart.fss.or.kr/dsap001/apikeyManagement.do
start_dt = '20000101' # start date
crp_cd = '*********' # company code(6 digit for listed companies and 8 digit for private companies which can be found at URL of "기업개황정보")
bsn_tp = 'D001' # https://dart.fss.or.kr/dsap001/guide.do "상세유형" 참조
page_set = '100' # 1 page당 출력 리스트
sub_menu = '2. 세부변동내역' # change "2. 세부변동내역" to specific submenu

url = f'{base_url}{auth_key}&start_dt={start_dt}&crp_cd={crp_cd}&bsn_tp={bsn_tp}&page_set={page_set}'
response = requests.get(url)
data = json.loads(response.content)
total_page = data['total_page']
page_list = data['list']

i = 1
while i < total_page:
    i += 1
    response = requests.get(url + '&page_no'+str(i))
    data = json.loads(response.content)
    page_list = page_list + data['list']

html_url = 'http://dart.fss.or.kr/dsaf001/main.do?rcpNo='
i = 0
j = len(page_list)
for each_doc in page_list:
    i += 1
    print(f"processing {i}/{j}")
    each_url = f"{html_url}{each_doc['rcp_no']}"
    req = requests.get(each_url)
    dcm_body = req.text.split(f'text: "{sub_menu}"')[1].split("viewDoc(")[1].split(")")[0] 
    dcm_body_list = dcm_body.replace("'", "").replace(" ", "").split(",")
    each_doc['dcmNo'] = dcm_body_list[1]
    each_doc['eleId'] = dcm_body_list[2]
    each_doc['offset'] = dcm_body_list[3]
    each_doc['length'] = dcm_body_list[4]

html_url = 'http://dart.fss.or.kr/report/viewer.do?rcpNo={0}&dcmNo={1}&eleId={2}&offset={3}&length={4}'
i = 0
columnlist = ['성명(명칭)', '생년월일 또는 사업자등록번호 등', '변동일', '취득/처분방법',
              '주식등의 종류', '변동전', '증감', '변동후', '취득/처분 단가', '비고'] # change columns as you change sub menu
df_all = pd.DataFrame()

for each_doc in page_list:
    i += 1
    print(f"processing {i}/{j}")
    each_url = html_url.format(each_doc['rcp_no'], each_doc['dcmNo'],
                               each_doc['eleId'], each_doc['offset'], each_doc['length'])
    req = requests.get(each_url)
    soup = BeautifulSoup(req.text, "html.parser")
    table = soup.find("table")
    df = pd.read_html(str(table))[0]
    df.columns = columnlist
    df.insert(0, '접수일', each_doc['rcp_dt']) #접수일을 column에 추가
    df_all = pd.concat([df_all, df])

df_all.reset_index(drop=True)
df_all.to_csv(r'data.csv') #data.csv로 저장
