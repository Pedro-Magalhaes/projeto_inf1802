# TweetCollectorApplication

Nesse exercício, usaremos a API de stream do Twitter para coletar Tweets que possuam determinadas palavras.
Adaptado para usr o kafka como sistema de mensageria

Para deixar o exercício mais interessante, a solução vai usar um serviço REST que inicia e finaliza a coleta de Tweets.
O serviço é iniciado com e o filtro aplicado é "rio de janeiro":

> curl http://localhost:8080/tweets/collector

Pode ser passado um parametro string opcional "q" indicando qual filtro deve ser usado nos tweets:

> curl http://localhost:8080/tweets/collector?q=Flamengo

O serviço é finalizado com:

> curl --request DELETE http://localhost:8090/tweets/collector


Sua classe, que implementa o *LifecycleManager*, deve ser encarregar de configurar a autenticação para uso da API do Twitter, 
conforme os dados de "Keys and tokens" no registro de sua aplicação no Twitter. Você pode usar variáveis de ambiente, conforme
o exemplo abaixo:

        String _consumerKey = System.getenv().get("TWITTER_CONSUMER_KEY");
        String _consumerSecret = System.getenv().get("TWITTER_CONSUMER_SECRET");
        String _accessToken = System.getenv().get("TWITTER_ACCESS_TOKEN");
        String _accessTokenSecret = System.getenv().get("TWITTER_ACCESS_TOKEN_SECRET");

        ConfigurationBuilder configurationBuilder = new ConfigurationBuilder();
        configurationBuilder.setOAuthConsumerKey(_consumerKey)
                .setOAuthConsumerSecret(_consumerSecret)
                .setOAuthAccessToken(_accessToken)
                .setOAuthAccessTokenSecret(_accessTokenSecret);

Se os certificados não estiverem instalados, devemos instalar e colocar a variavel de VM indicando
Lembrar de colocar a variavel de VM -Djavax.net.ssl.trustStore=/etc/ssl/certs/java/cacerts


### Kafka producer
 Meu Producer é criado(start) e fechado(stop) na TweetLifeCycleManager, a classe TweetListener passou a receber um producer
 como parametro e quando recebe um tweet cria o objeto Tweet e o envia para o tópico "tweet" do kafka
 A key utilizada para cada mensagem é o nome do usuário que enviou o tweet
