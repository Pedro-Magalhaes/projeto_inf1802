import javax.inject.Inject;
import javax.ws.rs.*;
import javax.ws.rs.core.Response;
import java.util.logging.Level;
import java.util.logging.Logger;

@Path("tweets/collector")
public class TweetConsumerResource {
     @Inject
     LifecycleManager manager;
    private static final Logger logger = Logger.getLogger(TweetConsumerResource.class.getName());

    /**
     * Inicia o serviço de Consumo de Tweets
     * @return 200 OK no caso de successo ou 500 em caso de falha
     */
    @GET
    public Response start( ) {
        Response r = null;

        try {
            manager.start();
            r = Response.ok("Consumer de Twitter iniciado")
                    .build();
        } catch (Exception ex) {
            logger.log(Level.SEVERE, null, ex);
            r = Response.serverError().build();
        }
        return r;
    }

    /**
     * Finaliza o serviço de Consumo de Tweets
     * @return 200 OK no caso de successo ou 500 em caso de falha
     */
    @DELETE
    public Response stop(@DefaultValue ("false")@QueryParam("q") String shouldDelete) {
        Response r = null;
        try {
            manager.stop(shouldDelete);
            r = Response.ok("Consumer de Twitter finalizado")
                    .build();
        } catch (Exception ex) {
            logger.log(Level.SEVERE, null, ex);
            r = Response.serverError().build();
        }
        return r;
    }
}