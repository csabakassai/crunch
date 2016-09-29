package com.doctusoft.crunch.util;

import com.doctusoft.dataops.Entries;
import com.doctusoft.java.Failsafe;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.api.client.json.GenericJson;
import com.google.api.services.bigquery.model.Table;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.coders.DeterministicStandardCoder;
import com.google.cloud.dataflow.sdk.util.CloudObject;
import com.google.common.base.Converter;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.primitives.Primitives;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.reflect.ReflectData;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.lang.annotation.Annotation;
import java.lang.reflect.AccessibleObject;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.SignStyle;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.regex.Pattern;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.google.cloud.dataflow.sdk.util.Structs.addString;
import static com.google.common.base.MoreObjects.firstNonNull;
import static java.time.format.DateTimeFormatter.ISO_LOCAL_TIME;
import static java.time.temporal.ChronoField.DAY_OF_MONTH;
import static java.time.temporal.ChronoField.MONTH_OF_YEAR;
import static java.time.temporal.ChronoField.YEAR;
import static java.util.Comparator.comparing;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public final class DomainClass<T> implements Serializable {

    private static final Collector IMMUTABLE_LIST_COLLECTOR = new Collector<Object, ImmutableList.Builder, ImmutableList>() {
        public Supplier<ImmutableList.Builder> supplier() {
            return () -> ImmutableList.builder();
        }

        public BiConsumer<ImmutableList.Builder, Object> accumulator() {
            return (b, e) -> b.add(e);
        }

        public BinaryOperator<ImmutableList.Builder> combiner() {
            return (a, b) -> a.addAll(b.build());
        }

        public Function<ImmutableList.Builder, ImmutableList> finisher() {
            return b -> b.build();
        }

        public Set<Characteristics> characteristics() {
            return Collections.emptySet();
        }
    };

    public static <T> DomainClass<T> of(@Nonnull Class<T> javaClass) {
        requireNonNull(javaClass);
        DomainClass domainClass = CACHE.get(javaClass);
        if (domainClass != null) return domainClass;
        domainClass = new DomainClass<>(javaClass);
        return firstNonNull(CACHE.putIfAbsent(javaClass, domainClass), domainClass);
    }

    private static final ConcurrentHashMap<Class, DomainClass> CACHE = new ConcurrentHashMap<>(256);

    private static final Function<String, String> namingStrategy = DomainClass::underscoredNaming;

    @Getter
    private final Class<T> javaClass;

    private final Method newBuilderMethod;

    private final Method buildMethod;

    @Getter(AccessLevel.PACKAGE)
    private final List<JavaPropertyNode> javaProperties;

    @Getter(AccessLevel.PACKAGE)
    private final List<BqFieldNode> bqFields;

    @Getter
    private final TableSchema bqSchema;

    @Getter
    private final String bqTableName;

    @Getter
    private final Schema avroSchema;

    @Getter
    private final AvroCoder<T> avroCoder;

    static <T> Class<T> checkValidDomainClass(Class<T> javaClass) {
        return javaClass;
    }

    private DomainClass(Class<T> javaClass) {
        this.javaClass = checkValidDomainClass(javaClass);
        Class<?> builderClass = findBuilderClass(javaClass);
        this.newBuilderMethod = findMethod(DomainClass::isStatic, javaClass, builderClass, "builder");
        this.buildMethod = findMethod(DomainClass::isPublic, builderClass, javaClass, "build");
        this.bqTableName = namingStrategy.apply(
            requireNonNull(javaClass.getSimpleName(), "Cannot use anonymous class"));
        this.javaProperties = Arrays
            .stream(this.javaClass.getMethods())
            .filter(DomainClass::isGetterMethod)
            .map(m -> toDataPoint(builderClass, m))
            .sorted(comparing(JavaPropertyNode::getUniformName))
            .collect(toList());
        Preconditions.checkArgument(javaProperties.size() > 0,  "Class has no properties: " + javaClass);
        Map<NodeClassification, List<JavaPropertyNode>> groups = javaProperties.stream()
            .collect(Collectors.groupingBy(BqFieldNode::getClassification));
        List<JavaPropertyNode> attributes = groups.getOrDefault(NodeClassification.ATTRIBUTE, ImmutableList.of());
        int nrOfAttributes = attributes
            .stream()
            .mapToInt(n -> n instanceof Embedded ? ((Embedded) n).dataClass.bqFields.size() : 1)
            .sum();
        List<JavaPropertyNode> records = ImmutableList.<JavaPropertyNode>builder()
            .addAll(groups.getOrDefault(NodeClassification.RECORD, ImmutableList.of()))
            .addAll(groups.getOrDefault(NodeClassification.REPEATED, ImmutableList.of()))
            .build();
        if (Math.min(nrOfAttributes, records.size()) <= 1) {
            this.bqFields = ImmutableList.<BqFieldNode>builder()
                .addAll(attributes)
                .addAll(records)
                .build();
        } else if (attributes.size() == 1) {
            Embedded embedded = (Embedded) attributes.get(0);
            this.bqFields = ImmutableList.<BqFieldNode>builder()
                .add(new Record(embedded.builderMethod.getDeclaringClass(), embedded.getterMethod, embedded.dataClass))
                .addAll(records)
                .build();
        } else {
            this.bqFields = ImmutableList.<BqFieldNode>builder()
                .add(new HeaderRecord(attributes))
                .addAll(records)
                .build();
        }
        this.bqSchema = new TableSchema().setFields(bqFields
            .stream()
            .flatMap(BqFieldNode::bqFields)
            .collect(toList()));
        this.avroSchema = Schema.createRecord(javaClass.getSimpleName(), null, getAvroNamespace(javaClass), false);
        avroSchema.setFields(javaProperties
            .stream()
            .map(JavaPropertyNode::createAvroField)
            .collect(Collectors.toList()));
        this.avroCoder = new AvroCoder<T>(this);
    }

    public Coder<T> getDefaultCoder() {
        return avroCoder;
    }

    public String getShardingTopicPostfix() {
        return javaClass.getSimpleName();
    }

    interface DataNode {

        String getUniformName();

        Converter<Object, Object> getCoder(DataStructure<?, ?> dataStructure);

        <U> void writeTo(@Nonnull Object source, @Nonnull U target, @Nonnull DataStructure<U, ?> dataStructure);

        <U> void readFrom(@Nonnull U source, @Nonnull Object target, @Nonnull DataStructure<U, ?> dataStructure);
    }

    private interface DataStructure<U, N extends DataNode> {

        Stream<N> nodesOf(DomainClass<?> domainClass);

        U newEncodedRecord(DomainClass<?> domainClass);

        @Nullable Object read(@Nonnull U encodedRecord, @Nonnull String property);

        void write(@Nonnull U encodedRecord, @Nonnull String property, @Nullable Object value);
    }

    private enum AvroDataStructure implements DataStructure<GenericRecord, JavaPropertyNode> {

        INSTANCE;

        public Stream<JavaPropertyNode> nodesOf(DomainClass<?> domainClass) { return domainClass.javaProperties.stream(); }

        public GenericRecord newEncodedRecord(DomainClass<?> domainClass) { return new GenericData.Record(domainClass.avroSchema); }

        @Nullable public Object read(@Nonnull GenericRecord encodedRecord, @Nonnull String property) { return encodedRecord.get(property); }

        public void write(@Nonnull GenericRecord encodedRecord, @Nonnull String property, @Nullable Object value) {
            if (value != null) {
                encodedRecord.put(property, value);
            }
        }
    }

    private enum BqDataStructure implements DataStructure<Map<String, Object>, BqFieldNode> {

        INSTANCE;

        public Stream<BqFieldNode> nodesOf(DomainClass<?> domainClass) { return domainClass.bqFields.stream(); }

        public Map<String, Object> newEncodedRecord(DomainClass<?> domainClass) { return new HashMap<>(); }

        public @Nullable Object read(@Nonnull Map<String, Object> encodedRecord, @Nonnull String property) { return encodedRecord.get(property); }

        public void write(@Nonnull Map<String, Object> encodedRecord, @Nonnull String property, @Nullable Object value) {
            if (value != null) {
                encodedRecord.put(property, value);
            }
        }
    }

    private interface BQ {
        String NULLABLE = "nullable";
        String REQUIRED = "required";
        String REPEATED = "repeated";
        String RECORD = "record";
    }

    private final DataNode rootNode = new DataNode() {

        public String getUniformName() { return ""; }

        public <U> void writeTo(@Nonnull Object source, @Nonnull U target, @Nonnull DataStructure<U, ?> dataStructure) {
            dataStructure
                .nodesOf(DomainClass.this)
                .forEach(n -> n.writeTo(source, target, dataStructure));
        }

        public <U> void readFrom(@Nonnull U source, @Nonnull Object target, @Nonnull DataStructure<U, ?> dataStructure) {
            dataStructure
                .nodesOf(DomainClass.this)
                .forEach(n -> n.readFrom(source, target, dataStructure));
        }

        public Converter<Object, Object> getCoder(DataStructure dataStructure) {
            return new Converter<Object, Object>() {

                protected Object doForward(Object o) { return encode((T) o, dataStructure, null); }

                protected Object doBackward(Object o) { return decode(o, dataStructure); }
            };
        }
    };

    private @Nonnull <U> U encode(@Nonnull T instance, @Nonnull DataStructure<U, ?> dataStructure, @Nullable U reuse) {
        requireNonNull(instance);
        requireNonNull(dataStructure);
        U encodedRecord = reuse != null ? reuse : dataStructure.newEncodedRecord(this);
        rootNode.writeTo(instance, encodedRecord, dataStructure);
        return encodedRecord;
    }

    @SneakyThrows
    private @Nonnull <U> T decode(@Nonnull U encodedRecord, @Nonnull DataStructure<U, ?> dataStructure) {
        requireNonNull(encodedRecord);
        requireNonNull(dataStructure);
        Object builder = requireNonNull(newBuilderMethod.invoke(null), "newBuilderMethod returned null");
        rootNode.readFrom(encodedRecord, builder, dataStructure);
        return (T) buildMethod.invoke(builder);
    }

    public @Nonnull GenericRecord writeAvro(@Nonnull T instance) {
        return encode(instance, AvroDataStructure.INSTANCE, null);
    }

    public @Nonnull T readAvro(@Nonnull GenericRecord record) {
        return decode(record, AvroDataStructure.INSTANCE);
    }

    public @Nonnull TableRow writeBq(@Nonnull T instance) {
        TableRow tableRow = new TableRow();
        encode(instance, BqDataStructure.INSTANCE, tableRow);
        return tableRow;
    }

    public @Nonnull T readBq(@Nonnull TableRow row) {
        return decode(row, BqDataStructure.INSTANCE);
    }

    public Table createTable(String projectId, String datasetId) {
        return createTableWithName(projectId, datasetId, bqTableName);
    }

    public Table createTableWithName(String projectId, String datasetId, String tableName) {
        requireNonNull(projectId, "projectId");
        requireNonNull(datasetId, "datasetId");
        return new Table()
            .setId(projectId + ":" + datasetId + "." + bqTableName)
            .setTableReference(new TableReference()
                .setProjectId(projectId)
                .setDatasetId(datasetId)
                .setTableId(tableName))
            .setSchema(bqSchema);

    }

    public Table createTableWithDailySharding(String projectId, String datasetId) {
        return createTableWithName(projectId, datasetId, bqTableName)
                .set("timePartitioning", new GenericJson().set("type", "DAY"));
    }

    public Table createTableWithShard(String projectId, String datasetId, String shard) {
        String tableName = bqTableName + "_" + shard;
        return createTableWithName(projectId, datasetId, tableName);
    }

    public TableSchema select(Iterable<String> fields) {
        Map<String, TableFieldSchema> fieldMap = bqSchema
            .getFields()
            .stream()
            .collect(Collectors.toMap(TableFieldSchema::getName, f -> f));
        List<TableFieldSchema> ordered = new ArrayList<>(fieldMap.size());
        fields.forEach(f -> ordered.add(requireNonNull(fieldMap.remove(f), () -> "No such field: " + f)));
        return new TableSchema().setFields(ordered);
    }

    private Stream<TableFieldSchema> bqFields() {
        return bqSchema.getFields().stream();
    }

    private Object writeReplace() { return new SerializedProxy<>(javaClass); }

    private static final class SerializedProxy<T> implements Serializable {

        private final Class<T> javaClass;

        public SerializedProxy(Class<T> javaClass) {
            this.javaClass = javaClass;
        }

        private Object readResolve() {
            return of(javaClass);
        }
    }

    /*
     * Helper methods & classes below:
     */

    interface BqFieldNode extends DataNode {

        NodeClassification getClassification();

        Stream<TableFieldSchema> bqFields();

    }

    private enum NodeClassification {ATTRIBUTE, RECORD, REPEATED}

    @Getter
    static abstract class JavaPropertyNode implements BqFieldNode {

        @Getter
        final String uniformName;
        final Method builderMethod;
        final Method getterMethod;
        final boolean markedNullable;
        @Getter
        private final Schema avroSchema;

        @SneakyThrows
        protected JavaPropertyNode(Class<?> builderClass, Method getterMethod, Schema nonNullSchema) {
            String fieldName = calculateFieldName(getterMethod);
            this.getterMethod = getterMethod;
            this.builderMethod = builderClass.getDeclaredMethod(fieldName, getterMethod.getReturnType());
            this.uniformName = namingStrategy.apply(fieldName);
            if (hasNullableAnnotation(getterMethod)) {
                this.markedNullable = true;
            } else {
                this.markedNullable = findField(getterMethod.getDeclaringClass(), fieldName)
                    .map(DomainClass::hasNullableAnnotation)
                    .orElse(false);
            }
            this.avroSchema = markedNullable ? ReflectData.makeNullable(nonNullSchema) : nonNullSchema;
        }

        @SneakyThrows
        protected final @Nullable Object get(@Nonnull Object instance) {
            requireNonNull(instance);
            Object returnedValue = getterMethod.invoke(instance);
            if (returnedValue == null && !markedNullable) {
                throw new IllegalArgumentException("Getter " + getterMethod + " returned null at: " + instance);
            }
            return returnedValue;
        }

        @SneakyThrows
        protected final void set(@Nonnull Object builder, @Nullable Object domainValue) {
            builderMethod.invoke(builder, domainValue);
        }

        public <U> void writeTo(@Nonnull Object source, @Nonnull U target, @Nonnull DataStructure<U, ?> dataStructure) {
            @Nullable Object domainValue = get(source);
            @Nullable Object uniformValue = getCoder(dataStructure).convert(domainValue);
            dataStructure.write(target, uniformName, uniformValue);
        }

        public <U> void readFrom(@Nonnull U source, @Nonnull Object target, @Nonnull DataStructure<U, ?> dataStructure) {
            @Nullable Object uniformValue = unwrap(dataStructure.read(source, uniformName));
            @Nullable Object domainValue = getCoder(dataStructure).reverse().convert(uniformValue);
            set(target, domainValue);
        }

        protected @Nullable Object unwrap(@Nullable Object uniformValue) { return uniformValue; }

        protected final String getBqFieldMode() { return markedNullable ? BQ.NULLABLE : BQ.REQUIRED; }

        private Schema.Field createAvroField() {
            return new Schema.Field(uniformName, getAvroSchema(), null, null);
        }
    }

    private static class SingleValue extends JavaPropertyNode {

        private final DataType dataType;

        private final List<TableFieldSchema> bqFields;

        public SingleValue(Class<?> builderClass, Method getterMethod, DataType dataType) {
            super(builderClass, getterMethod, dataType.getUniformDataType().getAvroSchema());
            this.dataType = dataType;
            this.bqFields = ImmutableList.of(new TableFieldSchema()
                .setName(uniformName)
                .setMode(getBqFieldMode())
                .setType(dataType.getUniformDataType().name())
            );
        }

        @Nullable protected Object unwrap(@Nullable Object uniformValue) {
            if (dataType != StaticDataType.STRING && uniformValue instanceof CharSequence) {
                return Strings.emptyToNull(uniformValue.toString().trim());
            }
            return uniformValue;
        }

        public NodeClassification getClassification() { return NodeClassification.ATTRIBUTE; }

        public Converter<Object, Object> getCoder(DataStructure<?, ?> dataStructure) { return dataType.getConverter(); }

        public Stream<TableFieldSchema> bqFields() { return bqFields.stream(); }
    }

    private static class Record extends JavaPropertyNode {

        private final DomainClass<?> recordClass;

        private final TableFieldSchema bqField;

        public Record(Class<?> builderClass, Method getterMethod, DomainClass recordClass) {
            super(builderClass, getterMethod, recordClass.avroSchema);
            this.recordClass = recordClass;
            this.bqField = new TableFieldSchema()
                .setName(uniformName)
                .setMode(getBqFieldMode())
                .setType(BQ.RECORD)
                .setFields(recordClass.bqSchema.getFields());
        }

        public NodeClassification getClassification() { return NodeClassification.RECORD; }

        public Converter<Object, Object> getCoder(DataStructure<?, ?> dataStructure) {
            return recordClass.rootNode.getCoder(dataStructure);
        }

        public Stream<TableFieldSchema> bqFields() {
            return Stream.of(bqField);
        }
    }

    private static Stream<TableFieldSchema> unwrapEmbeddedFields(JavaPropertyNode node) {
        if (node instanceof Embedded) {
            return ((Embedded) node).dataClass.bqFields();
        }
        return node.bqFields();
    }

    private static class Embedded extends JavaPropertyNode {

        private final DomainClass<?> dataClass;

        public Embedded(Class<?> builderClass, Method getterMethod, DomainClass dataClass) {
            super(builderClass, getterMethod, dataClass.avroSchema);
            this.dataClass = dataClass;
        }

        public NodeClassification getClassification() { return NodeClassification.ATTRIBUTE; }

        public Converter<Object, Object> getCoder(DataStructure dataStructure) {
            return dataClass.rootNode.getCoder(dataStructure);
        }

        public <U> void writeTo(@Nonnull Object source, @Nonnull U target, @Nonnull DataStructure<U, ?> dataStructure) {
            @Nullable Object domainValue = get(source);
            @Nullable Object uniformValue = getCoder(dataStructure).convert(domainValue);
            writeEncoded(uniformValue, target, dataStructure);
        }

        private <U> void writeEncoded(Object uniformValue, @Nonnull U target, @Nonnull DataStructure<U, ?> dataStructure) {
            if (dataStructure == BqDataStructure.INSTANCE) {
                if (uniformValue != null) {
                    Map<String, Object> targetMap = (Map<String, Object>) target; // safe to case here, since dataStructure is BQ
                    Entries
                        .forMap((Map<String, Object>) uniformValue)
                        .transformKeys(this::wrapFieldName)
                        .forEach(targetMap::put);
                }
            } else {
                dataStructure.write(target, uniformName, uniformValue);
            }
        }

        public <U> void readFrom(@Nonnull U source, @Nonnull Object target, @Nonnull DataStructure<U, ?> dataStructure) {
            @Nullable Object uniformValue = readEncoded(source, dataStructure);
            @Nullable Object domainValue = getCoder(dataStructure).reverse().convert(uniformValue);
            set(target, domainValue);
        }

        private @Nullable <U> Object readEncoded(@Nonnull U source, @Nonnull DataStructure<U, ?> dataStructure) {
            if (dataStructure == BqDataStructure.INSTANCE) {
                Map<String, Object> sourceMap = (Map<String, Object>) source; // safe to case here, since dataStructure is BQ
                Map<String, Object> unwrappedSource = new HashMap<>();
                dataClass.bqFields
                    .stream()
                    .forEach(n -> {
                        JavaPropertyNode node = (JavaPropertyNode) n;
                        String key = node.uniformName;
                        Object value = sourceMap.get(wrapFieldName(key));
                        unwrappedSource.put(key, value);
                    });
                return unwrappedSource;
            }
            return dataStructure.read(source, uniformName);
        }

        private String wrapFieldName(String fieldName) {
            return uniformName + "_" + fieldName;
        }

        private TableFieldSchema wrapField(TableFieldSchema field) {
            field.setName(wrapFieldName(field.getName()));
            if (markedNullable && field.getMode().equalsIgnoreCase(BQ.REQUIRED)) {
                field.setMode(BQ.NULLABLE);
            }
            return field;
        }

        public Stream<TableFieldSchema> bqFields() {
            return dataClass.javaProperties
                .stream()
                .flatMap(DomainClass::unwrapEmbeddedFields)
                .map(this::wrapField);
        }
    }

    private static abstract class Repeated extends JavaPropertyNode {

        private final TableFieldSchema bqField;

        protected Repeated(Class<?> builderClass, Method getterMethod, Schema recordSchema, List<TableFieldSchema> recordFields) {
            super(builderClass, getterMethod, Schema.createArray(recordSchema));
            this.bqField = new TableFieldSchema()
                .setName(uniformName)
                .setMode(BQ.REPEATED)
                .setType(BQ.RECORD)
                .setFields(recordFields);
        }

        public NodeClassification getClassification() { return NodeClassification.REPEATED; }

        public Stream<TableFieldSchema> bqFields() { return Stream.of(bqField); }

        public Converter<Object, Object> getCoder(DataStructure<?, ?> dataStructure) {
            return new Converter<Object, Object>() {

                protected Object doForward(Object o) {
                    Collection<Object> elements = (Collection) firstNonNull(o, ImmutableList.of());
                    Converter<Object, Object> elementCoder = getElementCoder(dataStructure);
                    return elements
                        .stream()
                        .map(elementCoder::convert)
                        .collect(Collectors.toList());
                }

                protected Object doBackward(Object o) {
                    Collection<Object> elements = (Collection) firstNonNull(o, ImmutableList.of());
                    Converter<Object, Object> elementDecoder = getElementCoder(dataStructure).reverse();
                    return elements
                        .stream()
                        .map(elementDecoder::convert)
                        .collect(IMMUTABLE_LIST_COLLECTOR);
                }
            };
        }

        protected abstract Converter<Object, Object> getElementCoder(DataStructure<?, ?> dataStructure);
    }

    private static final class RepeatedValue extends Repeated {

        private static final String UNIFORM_NAME = "value";

        private final DataType dataType;

        public RepeatedValue(Class<?> builderClass, Method getterMethod, DataType dataType) {
            super(builderClass, getterMethod, dataType.getUniformDataType().getAvroSchema(), ImmutableList.of(new TableFieldSchema()
                .setName(UNIFORM_NAME)
                .setMode(BQ.REQUIRED)
                .setType(dataType.getUniformDataType().name())
            ));
            this.dataType = dataType;
        }

        protected Converter<Object, Object> getElementCoder(DataStructure<?, ?> dataStructure) {
            if (dataStructure == BqDataStructure.INSTANCE) {
                return dataType.getConverter().andThen(BQ_WRAPPER);
            }
            return dataType.getConverter();
        }

        private static final Converter BQ_WRAPPER = new Converter<Object, Map<String, Object>>() {

            protected Map<String, Object> doForward(Object o) { return ImmutableMap.of(UNIFORM_NAME, o); }

            protected Object doBackward(Map<String, Object> o) { return o.get(UNIFORM_NAME); }

        };
    }

    private static final class RepeatedRecord extends Repeated {

        private final DomainClass recordClass;

        public RepeatedRecord(Class<?> builderClass, Method getterMethod, DomainClass<?> recordClass) {
            super(builderClass, getterMethod, recordClass.avroSchema, recordClass.bqSchema.getFields());
            this.recordClass = recordClass;
        }

        protected Converter<Object, Object> getElementCoder(DataStructure<?, ?> dataStructure) { return recordClass.rootNode.getCoder(dataStructure); }
    }

    @RequiredArgsConstructor
    private static final class HeaderRecord implements BqFieldNode {

        private static final String UNIFORM_NAME = "header";

        final List<JavaPropertyNode> properties;

        public NodeClassification getClassification() { return NodeClassification.RECORD; }

        public String getUniformName() { return UNIFORM_NAME; }

        public Stream<TableFieldSchema> bqFields() {
            return Stream.of(new TableFieldSchema()
                .setName(UNIFORM_NAME)
                .setMode(BQ.REQUIRED)
                .setType(BQ.RECORD)
                .setFields(properties
                    .stream()
                    .flatMap(JavaPropertyNode::bqFields)
                    .collect(toList()))
            );
        }

        public Converter<Object, Object> getCoder(DataStructure<?, ?> dataStructure) { throw new UnsupportedOperationException(); }

        public <U> void writeTo(@Nonnull Object source, @Nonnull U target, @Nonnull DataStructure<U, ?> dataStructure) {
            Failsafe.checkArgumentEquals(dataStructure, BqDataStructure.INSTANCE);
            U headerRecord = dataStructure.newEncodedRecord(null); // safe to pass null here, since BQ newEncodedRecord() creates an empty Map
            properties.forEach(n -> n.writeTo(source, headerRecord, dataStructure));
            dataStructure.write(target, UNIFORM_NAME, headerRecord);
        }

        public <U> void readFrom(@Nonnull U source, @Nonnull Object target, @Nonnull DataStructure<U, ?> dataStructure) {
            Failsafe.checkArgumentEquals(dataStructure, BqDataStructure.INSTANCE);
            U headerRecord = (U) dataStructure.read(source, UNIFORM_NAME);
            properties.forEach(n -> n.readFrom(headerRecord, target, dataStructure));
        }
    }

    interface DataType {

        Converter getConverter();

        UniformDataType getUniformDataType();

    }

    @Getter
    enum UniformDataType {

        INTEGER(Long.class, Schema.Type.LONG),
        FLOAT(Double.class, Schema.Type.DOUBLE),
        STRING(String.class, Schema.Type.STRING),
        BOOLEAN(Boolean.class, Schema.Type.BOOLEAN),
        TIMESTAMP(String.class, Schema.Type.STRING);

        private final Class<?> javaType;

        private final Schema avroSchema;

        private UniformDataType(Class<?> javaType, Schema.Type avroType) {
            this.javaType = requireNonNull(javaType);
            this.avroSchema = Schema.create(avroType);
        }
    }

    @Nonnull
    static <T> Converter<T, Object> requireConverter(Class<T> type) {
        DataType dataType = typeOf(type);
        if (dataType == null) {
            throw new IllegalArgumentException("No DataType for: " + type);
        }
        return dataType.getConverter();
    }

    @Nullable
    static DataType typeOf(Class<?> javaClass) {
        Class<?> typeClass = Primitives.wrap(javaClass);
        if (typeClass == Long.class) return StaticDataType.LONG;
        if (typeClass == Integer.class) return StaticDataType.INTEGER;
        if (typeClass == String.class) return StaticDataType.STRING;
        if (typeClass == Boolean.class) return StaticDataType.BOOLEAN;
        if (typeClass == BigDecimal.class) return StaticDataType.DECIMAL;
        if (typeClass == Double.class) return StaticDataType.DOUBLE;
        if (typeClass == LocalDate.class) return StaticDataType.LOCAL_DATE;
        if (typeClass == LocalDateTime.class) return StaticDataType.LOCAL_DATE_TIME;
        if (typeClass == Instant.class) return StaticDataType.INSTANT;
        if (typeClass.isEnum()) return new EnumType(typeClass);
        return null;
    }

    @Getter
    @RequiredArgsConstructor
    private enum StaticDataType implements DataType {

        INTEGER(UniformDataType.INTEGER) {
            public Long toUniformValue(@Nonnull Object value) { return ((Integer) value).longValue(); }

            public Integer toDomainValue(@Nonnull Object value) { return Math.toIntExact(toLong(value)); }
        },
        LONG(UniformDataType.INTEGER) {
            public Long toUniformValue(@Nonnull Object value) { return (Long) value; }

            public Long toDomainValue(@Nonnull Object value) { return toLong(value); }
        },
        STRING(UniformDataType.STRING) {
            public String toUniformValue(@Nonnull Object value) { return (String) value; }

            public String toDomainValue(@Nonnull Object value) { return value.toString(); }
        },
        BOOLEAN(UniformDataType.BOOLEAN) {
            public Boolean toUniformValue(@Nonnull Object value) {return (Boolean) value; }

            public Boolean toDomainValue(@Nonnull Object value) {return (Boolean) value; }
        },
        DECIMAL(UniformDataType.FLOAT) {
            public Double toUniformValue(@Nonnull Object value) { return ((BigDecimal) value).doubleValue(); }

            public BigDecimal toDomainValue(@Nonnull Object value) { return new BigDecimal(((Number) value).doubleValue()); }
        },
        DOUBLE(UniformDataType.FLOAT) {
            public Double toUniformValue(@Nonnull Object value) { return (Double) value; }

            public Double toDomainValue(@Nonnull Object value) { return (Double) value; }
        },
        INSTANT(UniformDataType.TIMESTAMP) {
            public String toUniformValue(@Nonnull Object value) { return Instant.class.cast(value).toString(); }

            public Instant toDomainValue(@Nonnull Object value) { return Instant.parse(value.toString()); }
        },
        LOCAL_DATE_TIME(UniformDataType.TIMESTAMP) {
            public String toUniformValue(@Nonnull Object value) { return ((LocalDateTime) value).toString(); }

            private final Pattern DECIMAL = Pattern.compile("-?[0-9.Ee]+");

            public LocalDateTime toDomainValue(@Nonnull Object value) {
                String str = value.toString();
                if (DECIMAL.matcher(str).matches()) {
                    BigDecimal timestamp = new BigDecimal(str);
                    long epoch = timestamp.longValue();
                    int nanos = timestamp.subtract(BigDecimal.valueOf(epoch)).movePointRight(9).intValue();
                    Instant instant = Instant.ofEpochSecond(epoch, nanos);
                    return LocalDateTime.ofInstant(instant, ZoneOffset.UTC);
                }
                if (str.contains(" ")) {
                    return ZonedDateTime.parse(str, BQ_DT_FMT)
                        .withZoneSameInstant(ZoneOffset.UTC)
                        .toLocalDateTime();
                }
                if (str.endsWith("Z")) str = str.substring(0, str.length() - 1);
                return LocalDateTime.parse(str);
            }
        },
        LOCAL_DATE(UniformDataType.STRING) {
            public Object toUniformValue(@Nonnull Object value) { return ((LocalDate) value).toString(); }

            public Object toDomainValue(@Nonnull Object value) { return LocalDate.parse(value.toString()); }
        };

        private static final DateTimeFormatter ZONE_FMT = new DateTimeFormatterBuilder()
            .parseCaseInsensitive()
            .appendLiteral(' ')
            .appendZoneOrOffsetId()
            .toFormatter();

        private static final DateTimeFormatter BQ_DATE_FMT = new DateTimeFormatterBuilder()
            .appendValue(YEAR, 4, 10, SignStyle.NORMAL)
            .appendLiteral('-')
            .appendValue(MONTH_OF_YEAR, 2)
            .appendLiteral('-')
            .appendValue(DAY_OF_MONTH, 2)
            .toFormatter();

        private static final DateTimeFormatter BQ_DT_FMT = new DateTimeFormatterBuilder()
            .parseCaseInsensitive()
            .append(BQ_DATE_FMT)
            .appendLiteral(' ')
            .append(ISO_LOCAL_TIME)
            .appendOptional(ZONE_FMT)
            .toFormatter()
            .withZone(ZoneOffset.UTC);

        private final UniformDataType uniformDataType;

        private final Converter<Object, Object> converter = new Converter<Object, Object>() {

            protected Object doForward(Object o) { return toUniformValue(o); }

            protected Object doBackward(Object o) { return toDomainValue(o); }

        };

        public abstract Object toUniformValue(@Nonnull Object value);

        public abstract Object toDomainValue(@Nonnull Object value);

        static Long toLong(Object value) {
            if (value instanceof CharSequence) {
                return Long.parseLong(value.toString());
            }
            if (value instanceof Long) {
                return (Long) value;
            }
            return Number.class.cast(value).longValue();
        }
    }

    @RequiredArgsConstructor
    private static final class EnumType<E extends Enum<E>> extends Converter<E, Object> implements DataType {

        private final Class<E> enumClass;

        protected Object doForward(E e) { return e.name(); }

        protected E doBackward(Object o) { return Enum.valueOf(enumClass, o.toString()); }

        public Converter getConverter() { return this; }

        public UniformDataType getUniformDataType() { return UniformDataType.STRING; }
    }

    public static final class AvroCoder<T> extends DeterministicStandardCoder<T> {

        @SuppressWarnings({ "unchecked", "rawtypes" })
        @JsonCreator
        public static AvroCoder<?> parseJson(@JsonProperty("javaClass") String javaClass) throws ClassNotFoundException {
            return DomainClass.of(Class.forName(javaClass)).avroCoder;
        }

        // Factories allocated by .get() are thread-safe and immutable.
        private static final EncoderFactory ENCODER_FACTORY = EncoderFactory.get();
        private static final DecoderFactory DECODER_FACTORY = DecoderFactory.get();

        @Getter
        private final DomainClass<T> domainClass;

        // Cache the old encoder/decoder and let the factories reuse them when possible. To be threadsafe,
        // these are ThreadLocal. This code does not need to be re-entrant as AvroCoder does not use
        // an inner coder.
        private final ThreadLocal<BinaryDecoder> decoder;
        private final ThreadLocal<BinaryEncoder> encoder;
        private final ThreadLocal<DatumWriter<GenericRecord>> writer;
        private final ThreadLocal<DatumReader<GenericRecord>> reader;

        AvroCoder(DomainClass<T> domainClass) {
            this.domainClass = requireNonNull(domainClass);

            // Decoder and Encoder start off null for each thread. They are allocated and potentially reused inside encode/decode.
            this.decoder = new ThreadLocal<>();
            this.encoder = new ThreadLocal<>();

            // Reader and writer are allocated once per thread and are "final" for thread-local Coder instance.
            this.reader = ThreadLocal.withInitial(() -> new GenericDatumReader<>(domainClass.getAvroSchema()));
            this.writer = ThreadLocal.withInitial(() -> new GenericDatumWriter<>(domainClass.getAvroSchema()));
        }

        public void encode(T value, OutputStream outStream, Context context) throws IOException {
            GenericRecord genericRecord = domainClass.writeAvro(value);
            // Get a BinaryEncoder instance from the ThreadLocal cache and attempt to reuse it.
            BinaryEncoder encoderInstance = ENCODER_FACTORY.directBinaryEncoder(outStream, encoder.get());
            // Save the potentially-new instance for reuse later.
            encoder.set(encoderInstance);
            writer.get().write(genericRecord, encoderInstance);
        }

        public T decode(InputStream inStream, Context context) throws IOException {
            // Get a BinaryDecoder instance from the ThreadLocal cache and attempt to reuse it.
            BinaryDecoder decoderInstance = DECODER_FACTORY.directBinaryDecoder(inStream, decoder.get());
            // Save the potentially-new instance for later.
            decoder.set(decoderInstance);
            GenericRecord genericRecord = reader.get().read(null, decoderInstance);
            return domainClass.readAvro(genericRecord);
        }

        public CloudObject asCloudObject() {
            CloudObject result = super.asCloudObject();
            addString(result, "javaClass", domainClass.getJavaClass().getName());
            return result;
        }

        public List<? extends Coder<?>> getCoderArguments() { return null; }

        public String getEncodingId() { return domainClass.getJavaClass().getName(); }

        private Object writeReplace() { return new SerializedProxy<>(domainClass.getJavaClass()); }

        private static final class SerializedProxy<T> implements Serializable {

            private final Class<T> javaClass;

            public SerializedProxy(Class<T> javaClass) { this.javaClass = javaClass; }

            private Object readResolve() { return DomainClass.of(javaClass).avroCoder; }
        }
    }

    /*
     * Static helper methods below:
     */

    static final String underscoredNaming(String javaName) {
        int ln = javaName.length();
        StringBuilder newName = new StringBuilder(ln);
        boolean lastLowerCase = false;
        boolean lastNumberic = false;
        char[] chars = javaName.toCharArray();
        for (int i = 0; i < ln; i++) {
            char c = chars[i];
            if (Character.isUpperCase(c)) {
                if (lastLowerCase) {
                    newName.append('_');
                }
                newName.append(Character.toLowerCase(c));
                lastLowerCase = false;
            } else {
                if (Character.isDigit(c) && !lastNumberic && i + 1 == ln) {
                    newName.append('_');
                }
                newName.append(c);
                lastLowerCase = true;
            }
            lastNumberic = Character.isDigit(c);
        }
        return newName.toString();
    }

    static String calculateFieldName(Method getterMethod) {
        String getterName = getterMethod.getName();
        String baseName = getterName.substring(getterName.startsWith("get") ? 3 : 2);
        return baseName.substring(0, 1).toLowerCase() + baseName.substring(1);
    }

    static boolean hasEmbedAnnotation(AccessibleObject accessibleObject) {
        Annotation[] declaredAnnotations = accessibleObject.getDeclaredAnnotations();
        return Arrays.stream(declaredAnnotations).anyMatch(ann -> ann.annotationType() == Embed.class);
    }

    static boolean hasNullableAnnotation(AccessibleObject accessibleObject) {
        Annotation[] declaredAnnotations = accessibleObject.getDeclaredAnnotations();
        return Arrays.stream(declaredAnnotations).anyMatch(DomainClass::isNullableAnnotation);
    }

    static boolean isNullableAnnotation(Annotation annotation) {
        Class<? extends Annotation> annotationClass = annotation.annotationType();
        return annotationClass == Nullable.class || annotationClass.getSimpleName().equals("Nullable");
    }

    static Optional<Field> findField(Method getterMethod) {
        String fieldName = calculateFieldName(getterMethod);
        return findField(getterMethod.getDeclaringClass(), fieldName);
    }

    static Optional<Field> findField(Class<?> javaClass, String fieldName) {
        try {
            StringBuilder buf = new StringBuilder(fieldName);
            buf.setCharAt(0, Character.toLowerCase(buf.charAt(0)));
            return Optional.of(javaClass.getField(buf.toString()));
        } catch (NoSuchFieldException e) {
            return Optional.empty();
        }
    }

    static boolean isGetterMethod(Method method) {
        return method.getParameterCount() == 0
            && isGetterName(method.getName())
            && isPublic(method)
            && method.getReturnType() != void.class;
    }

    static boolean isPublic(Method method) {
        return Modifier.isPublic(method.getModifiers());
    }

    static boolean isStatic(Method method) {
        return Modifier.isStatic(method.getModifiers());
    }

    static boolean isGetterName(String name) {
        if (name.startsWith("get")) {
            return !name.equals("getClass");
        }
        return name.startsWith("is");
    }

    static JavaPropertyNode toDataPoint(Class<?> builderClass, Method getterMethod) {
        Type returnType = getterMethod.getGenericReturnType();
        Class<?> javaClass = findClass(returnType);
        DataType dataType = typeOf(javaClass);
        if (dataType != null) {
            return new SingleValue(builderClass, getterMethod, dataType);
        }
        if (returnType instanceof ParameterizedType && Collection.class.isAssignableFrom(javaClass)) {
            Class<?> paramClass = findClass(((ParameterizedType) returnType).getActualTypeArguments()[0]);
            DataType paramType = typeOf(paramClass);
            if (paramType != null) {
                return new RepeatedValue(builderClass, getterMethod, paramType);
            }
            return new RepeatedRecord(builderClass, getterMethod, of(paramClass));
        }
        DomainClass recordClass = of(javaClass);
        if (findField(getterMethod).map(DomainClass::hasEmbedAnnotation).orElse(false)) {
            return new Embedded(builderClass, getterMethod, recordClass);
        }
        return new Record(builderClass, getterMethod, recordClass);
    }

    static String getAvroNamespace(Class<?> javaClass) {
        String space = javaClass.getPackage() == null ? "" : javaClass.getPackage().getName();
        if (javaClass.getEnclosingClass() != null) {
            space = javaClass.getEnclosingClass().getName() + "$";
        }
        return space;
    }

    static Class<?> findBuilderClass(Class<?> javaClass) {
        String builderClassName = javaClass.getSimpleName() + "Builder";
        for (Class<?> nestedClass : javaClass.getClasses()) {
            if (nestedClass.getSimpleName().equals(builderClassName)) {
                return nestedClass;
            }
        }
        throw new IllegalArgumentException("No builder class found in: " + javaClass);
    }

    static Class<?> findClass(Type type) {
        if (type instanceof Class) {
            return (Class<?>) type;
        }
        if (type instanceof ParameterizedType) {
            return findClass(((ParameterizedType) type).getRawType());
        }
        throw new IllegalArgumentException("Cannot handle type: " + type);
    }

    static Method findMethod(Predicate<Method> validate, Class<?> javaClass, Class<?> returnType, String methodName, Class<?>... parameterTypes) {
        try {
            Method method = javaClass.getDeclaredMethod(methodName, parameterTypes);
            Preconditions.checkArgument(returnType.isAssignableFrom(method.getReturnType()), "Return type mismatch: " + method);
            Preconditions.checkArgument(validate.test(method), "Method is invalid: " + method);
            return method;
        } catch (NoSuchMethodException e) {
            throw new IllegalArgumentException("No '" + methodName + "' method found: " + javaClass);
        }
    }

}
