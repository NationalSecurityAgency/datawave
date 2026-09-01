package datawave.microservice.annotationCache;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.google.protobuf.InvalidProtocolBufferException;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.map.IMap;
import com.hazelcast.map.listener.EntryAddedListener;
import com.hazelcast.map.listener.EntryUpdatedListener;

import datawave.annotation.protobuf.v1.Annotation;
import datawave.microservice.annotationCache.api.AnnotationMessageProto.AnnotationMessage;

/**
 * Listener on all updates to the instance maps
 */
@Component
public class AnnotationSyncListener implements EntryAddedListener<String,Object>, EntryUpdatedListener<String,Object>, HazelcastInstanceAware {
    private static Logger log = LoggerFactory.getLogger(AnnotationSyncListener.class);

    private HazelcastInstance instance;

    // automatically injected by spring
    @Override
    public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
        this.instance = hazelcastInstance;
    }

    @Override
    public void entryAdded(EntryEvent<String,Object> event) {
        if (event.getName().equals("annotations")) {
            if (!(event.getValue() instanceof AnnotationMessage)) {
                log.trace("unexpected value: " + event.getValue().getClass() + " " + event.getValue());
                return;
            }
            AnnotationMessage annotationMessage = (AnnotationMessage) event.getValue();
            Annotation annotation = null;
            try {
                annotation = Annotation.parseFrom(annotationMessage.getAnnotationBytes());
            } catch (InvalidProtocolBufferException e) {
                throw new RuntimeException(e);
            }
            log.info("syncing to docAnnotations");
            if (instance != null) {
                log.info("pushing to alt map");
                IMap<String,List<String>> annotationsMap = instance.getMap("docAnnotations");
                String docId = annotation.getDocumentId();
                List<String> annotationIds = annotationsMap.get(annotation.getDocumentId());
                if (annotationIds == null) {
                    annotationIds = new ArrayList<>();
                }
                annotationIds.add(event.getKey());
                annotationsMap.set(docId, annotationIds);
            }
        }
    }

    @Override
    public void entryUpdated(EntryEvent<String,Object> event) {
        if (event.getName().equals("annotations")) {
            if (!(event.getValue() instanceof Annotation)) {
                log.trace("unexpected value: " + event.getValue().getClass() + " " + event.getValue());
                return;
            }
            Annotation annotation = (Annotation) event.getValue();
            log.info("update syncing to docAnnotations");
            if (instance != null) {
                log.info("update pushing to alt map");
                IMap<String,List<String>> annotationsMap = instance.getMap("docAnnotations");
                String docId = annotation.getDocumentId();
                List<String> annotationIds = annotationsMap.get(annotation.getDocumentId());
                if (annotationIds == null) {
                    annotationIds = new ArrayList<>();
                }
                annotationIds.add(event.getKey());
                annotationsMap.set(docId, annotationIds);
            }
        }
    }
}
