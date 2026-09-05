VERSÃO DE RECUPERAÇÃO DAS ESTAÇÕES — SEM PROXY

Esta versão volta a usar diretamente:
wss://monitoramento.defesacivil.sc.gov.br/graphql
com o protocolo graphql-transport-ws.

Objetivo desta versão: primeiro recuperar e tornar visíveis as estações da Defesa Civil SC.
Ela não depende de /api, fetch local ou servidor intermediário.

A seleção automática tenta:
1) metadados Alto/Médio Vale;
2) bacia/nome Itajaí + posição a montante de Blumenau;
3) bacia/nome Itajaí;
4) todas as estações com chuva, apenas como último fallback.

Também responde a ping do servidor WebSocket.




