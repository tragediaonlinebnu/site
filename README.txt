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


Monitor de Rio do Sul integrado: nivel_riodosul.html, usando a API GraphQL/WebSocket oficial da Defesa Civil SC e a estação SDC Rio do Sul - 00013. O monitor de Blumenau permanece separado e inalterado.


Atualização: a variação por hora de Rio do Sul foi ajustada para comparar o nível atual com a leitura histórica mais próxima de 1 hora atrás, usando janela de tolerância de 35 minutos. O desconto local de 15 cm é aplicado também à leitura histórica, mantendo a variação correta.
