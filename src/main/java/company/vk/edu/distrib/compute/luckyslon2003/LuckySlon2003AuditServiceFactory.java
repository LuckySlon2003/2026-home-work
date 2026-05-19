package company.vk.edu.distrib.compute.luckyslon2003;

import company.vk.edu.distrib.compute.AuditService;
import company.vk.edu.distrib.compute.AuditServiceFactory;

public class LuckySlon2003AuditServiceFactory extends AuditServiceFactory {

    @Override
    protected AuditService doCreate(String bootstrapServers, String consumerGroupId) {
        return new LuckySlon2003AuditService(bootstrapServers, consumerGroupId);
    }
}
