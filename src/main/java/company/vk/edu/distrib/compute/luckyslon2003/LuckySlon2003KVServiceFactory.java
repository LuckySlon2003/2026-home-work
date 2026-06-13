package company.vk.edu.distrib.compute.luckyslon2003;

import company.vk.edu.distrib.compute.KVService;
import company.vk.edu.distrib.compute.KVServiceFactory;

import java.io.IOException;

public class LuckySlon2003KVServiceFactory extends KVServiceFactory {

    private static final int REPLICAS = 3;

    @Override
    protected KVService doCreate(int port) throws IOException {
        return new ReplicatedKVService(port, REPLICAS);
    }
}
