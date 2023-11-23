#========Biliotecas e mensagens de erro a serem utilizadas========#
from selenium.webdriver.firefox.options import Options
from selenium import webdriver
from bs4 import BeautifulSoup
from datetime import datetime
from time import sleep
import psycopg2 as pg
import pandas as pd

class XpathError(Exception):
    pass
class ClassNameError(Exception):
    pass


#===============Navegando pelo site===============#
#===ativando modo headless do selenium
firefox_options = Options()
firefox_options.add_argument('--headless')

driver = webdriver.Firefox(options=firefox_options)
url = 'https://www.amazon.com.br'
produto_pesquisa = 'celular' #Produto a serem extraídas as informações de precificacao

driver.get(url)
#===Definindo elementos de pesquisa de diferentes versoes do site
barra_pg1 = '/html/body/div[1]/header/div/div[1]/div[2]/div/form/div[3]/div[1]/input'
barra_pg2 = '//*[@id="nav-bb-search"]'

botao_pg1 = '/html/body/div[1]/header/div/div[1]/div[2]/div/form/div[4]/div/span/input'
botao_pg2 = '/html/body/div[1]/div[1]/div/div[2]/form/input'

#===Enviar caracteres para barra de pesquisa
try:
    driver.find_element('xpath', barra_pg1).send_keys(produto_pesquisa)
    sleep(2)
except:
    try:
        driver.find_element('xpath', barra_pg2).send_keys(produto_pesquisa)
        sleep(2)
    except:
        raise XpathError('Xpath da barra de pesquisa nao encontrado')

#===Clicar no botao de pesquisa
try:
    driver.find_element('xpath', botao_pg1).click()
    sleep(2)
except:
    try:
        driver.find_element('xpath', botao_pg2).click()
        sleep(2)
    except:
        raise XpathError('Xpath do botao de pesquisa nao encontrado')
print('Navegacao concluida.')

#===============Navegando entre paginas e extraindo html===============#
list_soups = [] #==Lista com o html de cada produto
prox_pag = ['a.s-pagination-item:nth-child(4)', 'a.s-pagination-item:nth-child(4)', 'a.s-pagination-item:nth-child(5)', 'a.s-pagination-item:nth-child(6)', 'a.s-pagination-item:nth-child(6)', 'a.s-pagination-item:nth-child(6)', 'fim']

for pagina in prox_pag:
    num = 3
    
    #===Extraindo HTML de cada produto (soup)
    for i in range(65):
        try:
            div_produto = driver.find_element('xpath', f'//*[@id="search"]/div[1]/div[1]/div/span[1]/div[1]/div[{num}]/div')
            html_content = div_produto.get_attribute('outerHTML')         
            soup = BeautifulSoup(html_content, 'html.parser')
            num += 1
        except:
            pass
        list_soups.append(soup)
    
    #===Proxima pagina
    try:
        driver.find_element('css selector',pagina).click()
        sleep(3)
    except:
        break
        
driver.close()

#===Verificacao se o xpath nao foi atualizado na pagina original
if len(list_soups) < 1:
    raise XpathError('Xpath de cada produto nao encontrado')
print('Extracao HTML concluida.')

#===============Extraidno preco e nome de cada produto===============#
list_prods = []
list_precs = []
for soup in list_soups:
    try:
        prod = soup.find('span', class_='a-size-base-plus a-color-base a-text-normal')
        prec = soup.find('span', class_='a-price-whole')
        
        prod = list(prod)
        prec = list(prec)  
        produto = prod[0]
        preco = prec[0]
        
        list_prods.append(produto)
        list_precs.append(preco)
    except:
        pass

#===Coluna termo pesquisa
termo_pesquisa = []
for i in range(len(list_precs)):
    termo_pesquisa.append(produto_pesquisa)

#===Coluna data pesquisa
data_pesquisa = []
for i in range(len(list_precs)):
    data = datetime.now()
    data_form = data.strftime("%Y-%m-%d")
    data_pesquisa.append(data_form)
print('Extracao dos dados concluida')

#===============Criacao do dataframe de dados===============#
colunas = ['produto', 'titulo_venda', 'preco', 'data_pesquisa']
dados = [termo_pesquisa, list_prods, list_precs, data_pesquisa]
df = pd.DataFrame(columns=colunas)

for i in range(len(colunas)):
    df[colunas[i]] = dados[i]

df['preco'] = df['preco'].astype('float')
df['data_pesquisa'] = pd.to_datetime(df['data_pesquisa'])


#================Upload do dataframe para o banco de dados================#
#===Estabelecendo conexao com banco de dados
host = 'localhost'
dbname = 'testeamazon'
user = '*********'
password = '*********'
sslmode = 'disable'
port = '5433'

conect_string = 'host={} user={} dbname={} password={} sslmode={}'.format(host, user, dbname, password, sslmode)
conec = pg.connect(conect_string)
cursor = conec.cursor()
print(f'Conexao com o banco de dados "{dbname}" estabelecia')

#===Criando tabela caso ainda nao exista
nome_tabela = 'dados_precificacao'
consulta_criar_tabela = f'''
CREATE TABLE IF NOT EXISTS {nome_tabela}(
    produto varchar,
    titulo_venda varchar,
    preco float,
    data_pesquisa date)
'''
cursor.execute(consulta_criar_tabela)
conec.commit()

#===Inserindo dado
for i in range(len(df)):
    consulta_inserir_dados = f'INSERT INTO {nome_tabela} (produto, titulo_venda, preco, data_pesquisa) VALUES (%s, %s, %s, %s)'
    valores = (df['produto'].iloc[i], df['titulo_venda'].iloc[i], df['preco'].iloc[i], df['data_pesquisa'].iloc[i])
    cursor.execute(consulta_inserir_dados, valores)
conec.commit()
print(f'Dados inseridos na tabela {nome_tabela} com sucesso')
exit()