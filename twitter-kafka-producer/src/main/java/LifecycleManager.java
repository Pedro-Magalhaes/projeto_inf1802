/**
 * Interface para gerência de ciclo de vida de um serviço
 */
public interface LifecycleManager {
    public void start(String filter);
    public void stop();
}
