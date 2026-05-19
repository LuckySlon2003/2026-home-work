package company.vk.edu.distrib.compute.luckyslon2003;

import company.vk.edu.distrib.compute.KVService;
import company.vk.edu.distrib.compute.KVServiceFactory;

import java.io.IOException;
import java.nio.file.Path;

public class LuckySlon2003AuditableKVServiceFactory extends KVServiceFactory {

    @Override
    protected KVService doCreate(int port) throws IOException {
        FileDao dao = new FileDao(dataDirectory());
        return new LuckySlon2003AuditableKVService(port, dao);
    }

    private static Path dataDirectory() {
        return Path.of(System.getProperty("java.io.tmpdir"), "luckyslon2003-audit-data");
    }
}
