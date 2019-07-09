# TweetConsumerApplication

Nesse exercício, usaremos o consumidor já implementado no exercício anterior, para produzir tweets no Kafka e
implementaremos o consumer para escutar o kafka e pegar os tweets

Para deixar o exercício mais interessante, a solução vai usar um serviço REST que inicia e finaliza o consumo de Tweets.

> curl http://localhost:8090/tweets/collector

O serviço é finalizado com:

> curl --request DELETE http://localhost:8090/tweets/collector


### TweetConsumer
 A classe TweetConsumer implementa a interface lifecycle e é responsável por dar start/stop no consumer, como o listen
 é feito através de um while, para não bloquear a chamada estamos usando uma thread para ficar fazendo o poll do kafka.
