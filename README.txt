VERSÃO FINAL — MONITORAMENTO E CALCULADORA INTELIGENTE

Esta versão volta a usar diretamente:
wss://monitoramento.defesacivil.sc.gov.br/graphql
com o protocolo graphql-transport-ws.

A página principal usa diretamente o WebSocket oficial da Defesa Civil SC com o protocolo graphql-transport-ws. Não há dependência de uma API externa de Rio do Sul.

A seleção de chuva é municipal e restrita às cidades definidas para o Alto e Médio Vale; o fallback não transforma chuva de outras regiões em dado contribuinte.

O monitor de Blumenau consulta a fonte oficial do nível e mantém fallbacks de leitura apenas para disponibilidade da página.






ATUALIZAÇÃO DO MODELO
- Chuva observada: somente pluviômetros via WebSocket da Defesa Civil SC.
- Chuva futura: somente ECMWF/Open-Meteo, separada da chuva observada.
- Barragens Sul/Oeste: estado das comportas e vertido entram como sinais experimentais; o ganho é ajustado pelo aprendizado.
- O histórico de barragens é salvo em data/dam_history.json para permitir estudar eventos de abertura.
- Previsões: +6h, +12h e +24h.


Versão final v4 da calculadora inteligente:
- chuva observada exclusivamente dos pluviômetros via WebSocket da Defesa Civil SC;
- chuva futura exclusivamente do ECMWF via Open-Meteo;
- nível de Blumenau exclusivamente da fonte oficial;
- barragens Sul/Oeste usam somente sinais operacionais observados e memória curta de eventos;
- não há dependência da API de Rio do Sul;
- aprendizado com atualização limitada e preservação do sinal da tendência (subida/descida);
- snapshots deduplicados e aprendizado reiniciado para evitar contaminar o novo modelo com registros antigos incompatíveis;
- chuva observada usada em cada horizonte somente até a janela disponível daquele horizonte (6h não usa chuva de 48/96h);
- conversão do nível de montante das barragens só ocorre quando a própria API informa unidade IBGE.


IMPORTANTE: a previsão de chuva ECMWF exibida no painel é apenas informativa. Ela NÃO participa da calculadora do nível do rio nem do aprendizado automático. A calculadora usa somente nível oficial, chuva observada dos pluviômetros, tendência do rio e sinais operacionais observados das barragens. ECMWF é exclusivamente informativo e não entra no cálculo nem no aprendizado.
