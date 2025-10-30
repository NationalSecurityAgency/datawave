package datawave.webservice.annotation;

import javax.ws.rs.core.Response;

public interface AnnotationManager {

    Response getAnnotationTypes(String idType, String id);

    Response getAnnotationsFor(String idType, String id);

    Response getAnnotationsByType(String idType, String id, String annotationType);

    Response getAnnotation(String idType, String id, String annotationId);

    Response addAnnotation(String idType, String id, String body);

    Response updateAnnotation(String idType, String id, String annotationId, String body);

    Response getAnnotationSegment(String idType, String id, String annotationId, String segmentId);

    Response addSegment(String idType, String id, String annotationId, String body);

    Response updateSegment(String idType, String id, String annotationId, String segmentId, String body);
}
