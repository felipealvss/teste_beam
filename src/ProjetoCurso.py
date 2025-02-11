import re
import apache_beam as beam # type: ignore
from apache_beam.io import ReadFromText # type: ignore
from apache_beam.options.pipeline_options import PipelineOptions # type: ignore
import pyarrow # type: ignore

options = PipelineOptions(argv=None)
pipeline = beam.Pipeline(options=options)

# Dados arquivo: casos_dengue.txt
arquivo_dengue = "casos_dengue.txt"

colunas_dengue = [
    'id', 'data_iniSE', 'casos', 'ibge_code', 'cidade', 'uf', 'cep', 'latitude', 'longitude'
]

# Dados arquivo: chuvas.csv
arquivo_chuvas = "chuvas.csv"

# Dados arquivo: resultado_final
header_res_final = "uf|ano|mes|chuva|dengue"

schema = [('uf', pyarrow.string()),
          ('ano', pyarrow.int32()),
          ('mes', pyarrow.int32()),
          ('chuvas', pyarrow.float32()),
          ('dengue', pyarrow.float32())]

# Funcoes arquivo: casos_dengue.txt
def list_to_dict(linha, cabecalho):
    """
    Recebe duas listas
    Retorna um dicionário
    """
    return dict(zip(cabecalho, linha))

def data_year_month(linha):
    """
    Recebe um dicionario
    Cria um nova coluna no formato ANO-MES
    Retorna dicionario com adição da coluna com ano e mês
    """
    linha['ano_mes'] = "-".join(linha['data_iniSE'].split('-')[:2])
    return linha

def uf_key(linha):
    """
    Recebe um dicionario
    Retorna uma tupla no formato: (uf, dicionario)
    """
    chave = linha['uf']
    return (chave, linha)

def desease_cases(linha):
    """
    Recebe tupla no formato (uf, dicionario)
    Retorna tupla no formato: (uf-ano-mes, casos)
    """
    uf, tupla = linha
    for t in tupla:
        if bool(re.search(r'\d', t['casos'])): # Verifica se é um tipo Float
            yield (f"{uf}-{t['ano_mes']}", round(float(t['casos']), 1))
        else:
            yield (f"{uf}-{t['ano_mes']}", 0.0)

# Funcoes arquivo: chuvas.csv
def adjust_month_uf(linha):
    """
    Recebe uma lista
    Retorna uma tupla no formato: (uf-ano-mes, casos)
    """
    data, mm, uf = linha
    ano_mes = "-".join(data.split('-')[:2])
    chave = f"{uf}-{ano_mes}"
    if float(mm) < 0:
        mm = 0.0
    else:
        mm = round(float(mm), 1)
    return (chave, mm)

# Ordenar resultados
def sorter_data(elementos):
    chave, valores = elementos
    dengue = sorted(valores['dengue'])
    chuvas = sorted(valores['chuvas'])
    return (chave, {'dengue': dengue, 'chuvas': chuvas})

def null_data_filter(linha):
    """
    Recebe um dicionario
    Retorna True se houver dados nulos
    """
    chave, valores = linha
    # if all(
    #     valores['dengue'],
    #     valores['chuvas']
    # ):
    if valores['dengue'] and valores['chuvas']:
        return True
    return False

def unzip_data(linha):
    """
    Recebe o dicionario
    Retorna os dados separados, de forma descompactada
    """
    chave, valores = linha
    chuva = round(valores['chuvas'][0], 1)
    dengue = round(valores['dengue'][0], 1)
    uf, ano, mes = chave.split('-')
    return (uf, int(ano), int(mes), chuva, dengue)

def format_delimited_line(linha, delimitador):
    """
    Recebe uma tupla e um delimitador
    Retorna uma string no formato delimitado
    """
    return delimitador.join(map(str, linha))

def parquet_data(elem):
   """
   Receber uma tupla ('CE-2015-11', {'chuvas': [0.4], 'dengue': [21.0]})
   Retornar um dicionário {'uf': 'CE', 'ano': 2015, 'mes': 11, 'chuvas': 0.4, 'dengue': 21.0}
   """
   chave, dados = elem
   chuva = dados['chuvas'][0]
   dengue = dados['dengue'][0]
   uf, ano, mes = chave.split('-')
   chaves = ['uf', 'ano', 'mes', 'chuvas', 'dengue']
   resultado = dict(zip(chaves, [uf, int(ano), int(mes), chuva, dengue]))
   return resultado

# Pcollection e Pipeline

assunto_dengue = "dengue"
dengue = (

    pipeline
    | f"{assunto_dengue} - Leitura do dataset" >> ReadFromText("casos_dengue.txt", skip_header_lines=1)

    | f"{assunto_dengue} - Linha de texto para lista" >> beam.Map(lambda x: x.split('|'))

    | f"{assunto_dengue} - Lista para dicionario" >> beam.Map(list_to_dict, colunas_dengue)

    | f"{assunto_dengue} - Criar coluna com padrão ANO-MES" >> beam.Map(data_year_month)

    | f"{assunto_dengue} - Criar chave por estado" >> beam.Map(uf_key)

    | f"{assunto_dengue} - Agrupar por estado" >> beam.GroupByKey()

    | f"{assunto_dengue} - Descompactar casos de dengue" >> beam.FlatMap(desease_cases)

    | f"{assunto_dengue} - Somar casos pela chave" >> beam.CombinePerKey(sum)

    #| f"{assunto_dengue} - Exibe informações" >> beam.Map(print)
)

assunto_chuvas = "chuvas"
chuvas = (

    pipeline
    | f"{assunto_chuvas} - Leitura do dataset de chuvas" >> ReadFromText("chuvas.csv", skip_header_lines=1)

    | f"{assunto_chuvas} - Linha de texto para lista" >> beam.Map(lambda x: x.split(','))

    | f"{assunto_chuvas} - Lista para tupla no padrão UF-ANO-MES" >> beam.Map(adjust_month_uf)

    | f"{assunto_chuvas} - Somar casos pela chave" >> beam.CombinePerKey(sum)

    #| f"{assunto_chuvas} - Exibe informações" >> beam.Map(print)
)

assunto_uniao = "uniao"
uniao = (
    #(dengue, chuvas)
    #| "Empilha as informações" >> beam.Flatten()
    #| "Agrupa informações" >> beam.GroupByKey()

    {'dengue': dengue, 'chuvas': chuvas}

    | "Agrupa informações" >> beam.CoGroupByKey()

    | "Filtrar dados nulos" >> beam.Filter(null_data_filter)

    # FLUXO CSV
    #| "Ordenar resultados" >> beam.Map(sorter_data)

    #| "Descompactar dados" >> beam.Map(unzip_data)

    #| "Estrutura linha delimidata" >> beam.Map(format_delimited_line, '|')

    #| "Salvar resultados em arquivo CSV" >> beam.io.WriteToText("resultado_final", file_name_suffix=".csv", header=header_res_final, num_shards=1)

    # FLUXO PARQUET
    | "Formatar dados para Parquet" >> beam.Map(parquet_data)

    | "Salvar resultados em arquivo Parquet" >> beam.io.WriteToParquet("resultado_final", file_name_suffix=".parquet", schema=pyarrow.schema(schema))

    #| "Mostrar resultados" >> beam.Map(print)

)

pipeline.run()
