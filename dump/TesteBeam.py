import apache_beam as beam
from datetime import datetime
import logging

# Configurando o logging
logging.basicConfig(level=logging.INFO)

def transform_data(row):
    # Extrai nome e idade da linha
    nome, idade = row
    ano_atual = datetime.now().year
    ano_nascimento = ano_atual - int(idade)
    
    # Logando a transformação
    logging.info(f'Transformando: {row} -> Ano de Nascimento: {ano_nascimento}')
    
    return {
        'nome': nome,
        'ano_nascimento': ano_nascimento
    }

# Definindo o pipeline
with beam.Pipeline() as pipeline:
    (
        # Lê o arquivo CSV
        pipeline
        | 'Ler CSV' >> beam.io.ReadFromText('input.csv')
        
        # Divide cada linha em uma lista
        | 'Dividir Linhas' >> beam.Map(lambda line: line.split(','))
        
        # Remove a linha de cabeçalho
        | 'Remover Cabeçalho' >> beam.Filter(lambda line: line[0] != 'nome')
        
        # Aplica a transformação para calcular o ano de nascimento
        | 'Transformar Dados' >> beam.Map(transform_data)
        
        # Formata os dados para CSV
        | 'Formatar para CSV' >> beam.Map(lambda record: f"{record['nome']},{record['ano_nascimento']}")
        
        # Escreve o resultado em um novo arquivo CSV
        | 'Escrever CSV' >> beam.io.WriteToText('output', file_name_suffix='.csv')
    )
