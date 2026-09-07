MONITORAMENTO HIDROMETEOROLÓGICO — VALE DO ITAJAÍ

Motor hidrológico de previsão e memória para Blumenau, baseado em chuva observada.

O projeto roda em GitHub Pages + GitHub Actions. O workflow executa a cada 15 minutos, coleta dados oficiais e atualiza os arquivos JSON consumidos pelo painel.

ARQUITETURA
- WebSocket oficial da Defesa Civil SC para chuva, nível e barragens.
- Fonte oficial do nível de Blumenau para o histórico do rio.
- ECMWF IFS via Open-Meteo pode aparecer apenas como informação meteorológica no painel; não entra no cálculo nem no aprendizado hidrológico.
- Histórico de níveis, chuva, barragens, snapshots e eventos em data/.
- smart_model.py: modelo hidrológico de tendência + chuva observada + barragens + analogia histórica.
- hydrology.py: detector de eventos, resposta da bacia, marcos de cota e estatísticas hidrológicas.

EVENTOS HIDROLÓGICOS
O motor cria automaticamente um evento quando detecta início de chuva relevante ou nível de Blumenau >= 5,0 m.
Durante o evento registra:
- início e gatilho;
- nível inicial e pico;
- chuva observada em 3/6/12/24/48/96 h;
- tempo aproximado entre início da chuva e resposta sustentada;
- tempo até 4, 5, 6 e 8 m;
- tempo até o pico;
- duração do evento;
- subida total em metros;
- subida por 100 mm de chuva em 48 h, quando houver dados suficientes.

MEMÓRIA / APRENDIZADO
Eventos encerrados passam a alimentar a analogia histórica. O modelo procura eventos com chuva semelhante e estima a fração da subida esperada em cada horizonte.

O sistema também calcula MAE, RMSE e viés por horizonte. A comparação de previsões usa uma tolerância de até 20 minutos em relação à leitura oficial para evitar avaliações distorcidas.

IMPORTANTE
- Isto é um modelo experimental/operacional de apoio, não um modelo oficial de previsão de cheias.
- Relações chuva → nível são aprendidas a partir dos eventos realmente registrados; não são uma lei física fixa.
- Barragens entram como sinal operacional conservador e não como conversão direta de comportas em metros de Blumenau.
- Alertas oficiais continuam sendo responsabilidade das autoridades competentes.
