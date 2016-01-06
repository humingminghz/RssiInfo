// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: ShopStore.proto

package com.palmap.rssi.message;

public final class ShopStore {
  private ShopStore() {}
  public static void registerAllExtensions(
          com.google.protobuf.ExtensionRegistry registry) {
  }
  public interface VisitorOrBuilder extends
          // @@protoc_insertion_point(interface_extends:palmapmsg.Visitor)
          com.google.protobuf.MessageOrBuilder {

    /**
     * <code>optional bytes phone_mac = 1;</code>
     *
     * <pre>
     * </pre>
     */
    boolean hasPhoneMac();
    /**
     * <code>optional bytes phone_mac = 1;</code>
     *
     * <pre>
     * </pre>
     */
    com.google.protobuf.ByteString getPhoneMac();

    /**
     * <code>optional int32 scene_id = 2;</code>
     *
     * <pre>
     * </pre>
     */
    boolean hasSceneId();
    /**
     * <code>optional int32 scene_id = 2;</code>
     *
     * <pre>
     * </pre>
     */
    int getSceneId();

    /**
     * <code>optional int32 location_id = 3;</code>
     *
     * <pre>
     * </pre>
     */
    boolean hasLocationId();
    /**
     * <code>optional int32 location_id = 3;</code>
     *
     * <pre>
     * </pre>
     */
    int getLocationId();

    /**
     * <code>optional int32 user_type = 4;</code>
     *
     * <pre>
     * </pre>
     */
    boolean hasUserType();
    /**
     * <code>optional int32 user_type = 4;</code>
     *
     * <pre>
     * </pre>
     */
    int getUserType();

    /**
     * <code>optional bytes phone_brand = 5;</code>
     *
     * <pre>
     * </pre>
     */
    boolean hasPhoneBrand();
    /**
     * <code>optional bytes phone_brand = 5;</code>
     *
     * <pre>
     * </pre>
     */
    com.google.protobuf.ByteString getPhoneBrand();

    /**
     * <code>repeated uint64 time_stamp = 6;</code>
     *
     * <pre>
     * ????
     * </pre>
     */
    java.util.List<Long> getTimeStampList();
    /**
     * <code>repeated uint64 time_stamp = 6;</code>
     *
     * <pre>
     * </pre>
     */
    int getTimeStampCount();
    /**
     * <code>repeated uint64 time_stamp = 6;</code>
     *
     * <pre>
     * </pre>
     */
    long getTimeStamp(int index);

    /**
     * <code>repeated sint32 rssi = 7;</code>
     *
     * <pre>
     * rssi???
     * </pre>
     */
    java.util.List<Integer> getRssiList();
    /**
     * <code>repeated sint32 rssi = 7;</code>
     *
     * <pre>
     * </pre>
     */
    int getRssiCount();
    /**
     * <code>repeated sint32 rssi = 7;</code>
     *
     * <pre>
     * </pre>
     */
    int getRssi(int index);
  }
  /**
   * Protobuf type {@code palmapmsg.Visitor}
   *
   * <pre>
   *rdd
   * </pre>
   */
  public static final class Visitor extends
          com.google.protobuf.GeneratedMessage implements
          // @@protoc_insertion_point(message_implements:palmapmsg.Visitor)
          VisitorOrBuilder {
    // Use Visitor.newBuilder() to construct.
    private Visitor(com.google.protobuf.GeneratedMessage.Builder<?> builder) {
      super(builder);
      this.unknownFields = builder.getUnknownFields();
    }
    private Visitor(boolean noInit) { this.unknownFields = com.google.protobuf.UnknownFieldSet.getDefaultInstance(); }

    private static final Visitor defaultInstance;
    public static Visitor getDefaultInstance() {
      return defaultInstance;
    }

    public Visitor getDefaultInstanceForType() {
      return defaultInstance;
    }

    private final com.google.protobuf.UnknownFieldSet unknownFields;
    @Override
    public final com.google.protobuf.UnknownFieldSet
    getUnknownFields() {
      return this.unknownFields;
    }
    private Visitor(
            com.google.protobuf.CodedInputStream input,
            com.google.protobuf.ExtensionRegistryLite extensionRegistry)
            throws com.google.protobuf.InvalidProtocolBufferException {
      initFields();
      int mutable_bitField0_ = 0;
      com.google.protobuf.UnknownFieldSet.Builder unknownFields =
              com.google.protobuf.UnknownFieldSet.newBuilder();
      try {
        boolean done = false;
        while (!done) {
          int tag = input.readTag();
          switch (tag) {
            case 0:
              done = true;
              break;
            default: {
              if (!parseUnknownField(input, unknownFields,
                      extensionRegistry, tag)) {
                done = true;
              }
              break;
            }
            case 10: {
              bitField0_ |= 0x00000001;
              phoneMac_ = input.readBytes();
              break;
            }
            case 16: {
              bitField0_ |= 0x00000002;
              sceneId_ = input.readInt32();
              break;
            }
            case 24: {
              bitField0_ |= 0x00000004;
              locationId_ = input.readInt32();
              break;
            }
            case 32: {
              bitField0_ |= 0x00000008;
              userType_ = input.readInt32();
              break;
            }
            case 42: {
              bitField0_ |= 0x00000010;
              phoneBrand_ = input.readBytes();
              break;
            }
            case 48: {
              if (!((mutable_bitField0_ & 0x00000020) == 0x00000020)) {
                timeStamp_ = new java.util.ArrayList<Long>();
                mutable_bitField0_ |= 0x00000020;
              }
              timeStamp_.add(input.readUInt64());
              break;
            }
            case 50: {
              int length = input.readRawVarint32();
              int limit = input.pushLimit(length);
              if (!((mutable_bitField0_ & 0x00000020) == 0x00000020) && input.getBytesUntilLimit() > 0) {
                timeStamp_ = new java.util.ArrayList<Long>();
                mutable_bitField0_ |= 0x00000020;
              }
              while (input.getBytesUntilLimit() > 0) {
                timeStamp_.add(input.readUInt64());
              }
              input.popLimit(limit);
              break;
            }
            case 56: {
              if (!((mutable_bitField0_ & 0x00000040) == 0x00000040)) {
                rssi_ = new java.util.ArrayList<Integer>();
                mutable_bitField0_ |= 0x00000040;
              }
              rssi_.add(input.readSInt32());
              break;
            }
            case 58: {
              int length = input.readRawVarint32();
              int limit = input.pushLimit(length);
              if (!((mutable_bitField0_ & 0x00000040) == 0x00000040) && input.getBytesUntilLimit() > 0) {
                rssi_ = new java.util.ArrayList<Integer>();
                mutable_bitField0_ |= 0x00000040;
              }
              while (input.getBytesUntilLimit() > 0) {
                rssi_.add(input.readSInt32());
              }
              input.popLimit(limit);
              break;
            }
          }
        }
      } catch (com.google.protobuf.InvalidProtocolBufferException e) {
        throw e.setUnfinishedMessage(this);
      } catch (java.io.IOException e) {
        throw new com.google.protobuf.InvalidProtocolBufferException(
                e.getMessage()).setUnfinishedMessage(this);
      } finally {
        if (((mutable_bitField0_ & 0x00000020) == 0x00000020)) {
          timeStamp_ = java.util.Collections.unmodifiableList(timeStamp_);
        }
        if (((mutable_bitField0_ & 0x00000040) == 0x00000040)) {
          rssi_ = java.util.Collections.unmodifiableList(rssi_);
        }
        this.unknownFields = unknownFields.build();
        makeExtensionsImmutable();
      }
    }
    public static final com.google.protobuf.Descriptors.Descriptor
    getDescriptor() {
      return com.palmap.rssi.message.ShopStore.internal_static_palmapmsg_Visitor_descriptor;
    }

    protected com.google.protobuf.GeneratedMessage.FieldAccessorTable
    internalGetFieldAccessorTable() {
      return com.palmap.rssi.message.ShopStore.internal_static_palmapmsg_Visitor_fieldAccessorTable
              .ensureFieldAccessorsInitialized(
                      com.palmap.rssi.message.ShopStore.Visitor.class, com.palmap.rssi.message.ShopStore.Visitor.Builder.class);
    }

    public static com.google.protobuf.Parser<Visitor> PARSER =
            new com.google.protobuf.AbstractParser<Visitor>() {
              public Visitor parsePartialFrom(
                      com.google.protobuf.CodedInputStream input,
                      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
                      throws com.google.protobuf.InvalidProtocolBufferException {
                return new Visitor(input, extensionRegistry);
              }
            };

    @Override
    public com.google.protobuf.Parser<Visitor> getParserForType() {
      return PARSER;
    }

    private int bitField0_;
    public static final int PHONE_MAC_FIELD_NUMBER = 1;
    private com.google.protobuf.ByteString phoneMac_;
    /**
     * <code>optional bytes phone_mac = 1;</code>
     *
     * <pre>
     * </pre>
     */
    public boolean hasPhoneMac() {
      return ((bitField0_ & 0x00000001) == 0x00000001);
    }
    /**
     * <code>optional bytes phone_mac = 1;</code>
     *
     * <pre>
     * ??????? mac ???
     * </pre>
     */
    public com.google.protobuf.ByteString getPhoneMac() {
      return phoneMac_;
    }

    public static final int SCENE_ID_FIELD_NUMBER = 2;
    private int sceneId_;
    /**
     * <code>optional int32 scene_id = 2;</code>
     *
     * <pre>
     * </pre>
     */
    public boolean hasSceneId() {
      return ((bitField0_ & 0x00000002) == 0x00000002);
    }
    /**
     * <code>optional int32 scene_id = 2;</code>
     *
     * <pre>
     * </pre>
     */
    public int getSceneId() {
      return sceneId_;
    }

    public static final int LOCATION_ID_FIELD_NUMBER = 3;
    private int locationId_;
    /**
     * <code>optional int32 location_id = 3;</code>
     *
     * <pre>
     * </pre>
     */
    public boolean hasLocationId() {
      return ((bitField0_ & 0x00000004) == 0x00000004);
    }
    /**
     * <code>optional int32 location_id = 3;</code>
     *
     * <pre>
     * </pre>
     */
    public int getLocationId() {
      return locationId_;
    }

    public static final int USER_TYPE_FIELD_NUMBER = 4;
    private int userType_;
    /**
     * <code>optional int32 user_type = 4;</code>
     *
     * <pre>
     * </pre>
     */
    public boolean hasUserType() {
      return ((bitField0_ & 0x00000008) == 0x00000008);
    }
    /**
     * <code>optional int32 user_type = 4;</code>
     *
     * <pre>
     * </pre>
     */
    public int getUserType() {
      return userType_;
    }

    public static final int PHONE_BRAND_FIELD_NUMBER = 5;
    private com.google.protobuf.ByteString phoneBrand_;
    /**
     * <code>optional bytes phone_brand = 5;</code>
     *
     * <pre>
     * </pre>
     */
    public boolean hasPhoneBrand() {
      return ((bitField0_ & 0x00000010) == 0x00000010);
    }
    /**
     * <code>optional bytes phone_brand = 5;</code>
     *
     * <pre
     * </pre>
     */
    public com.google.protobuf.ByteString getPhoneBrand() {
      return phoneBrand_;
    }

    public static final int TIME_STAMP_FIELD_NUMBER = 6;
    private java.util.List<Long> timeStamp_;
    /**
     * <code>repeated uint64 time_stamp = 6;</code>
     *
     * <pre>
     * </pre>
     */
    public java.util.List<Long>
    getTimeStampList() {
      return timeStamp_;
    }
    /**
     * <code>repeated uint64 time_stamp = 6;</code>
     *
     * <pre>
     * </pre>
     */
    public int getTimeStampCount() {
      return timeStamp_.size();
    }
    /**
     * <code>repeated uint64 time_stamp = 6;</code>
     *
     * <pre>
     * </pre>
     */
    public long getTimeStamp(int index) {
      return timeStamp_.get(index);
    }

    public static final int RSSI_FIELD_NUMBER = 7;
    private java.util.List<Integer> rssi_;
    /**
     * <code>repeated sint32 rssi = 7;</code>
     *
     * <pre>
     * </pre>
     */
    public java.util.List<Integer>
    getRssiList() {
      return rssi_;
    }
    /**
     * <code>repeated sint32 rssi = 7;</code>
     *
     * <pre>
     * </pre>
     */
    public int getRssiCount() {
      return rssi_.size();
    }
    /**
     * <code>repeated sint32 rssi = 7;</code>
     *
     * <pre>
     * </pre>
     */
    public int getRssi(int index) {
      return rssi_.get(index);
    }

    private void initFields() {
      phoneMac_ = com.google.protobuf.ByteString.EMPTY;
      sceneId_ = 0;
      locationId_ = 0;
      userType_ = 0;
      phoneBrand_ = com.google.protobuf.ByteString.EMPTY;
      timeStamp_ = java.util.Collections.emptyList();
      rssi_ = java.util.Collections.emptyList();
    }
    private byte memoizedIsInitialized = -1;
    public final boolean isInitialized() {
      byte isInitialized = memoizedIsInitialized;
      if (isInitialized == 1) return true;
      if (isInitialized == 0) return false;

      memoizedIsInitialized = 1;
      return true;
    }

    public void writeTo(com.google.protobuf.CodedOutputStream output)
            throws java.io.IOException {
      getSerializedSize();
      if (((bitField0_ & 0x00000001) == 0x00000001)) {
        output.writeBytes(1, phoneMac_);
      }
      if (((bitField0_ & 0x00000002) == 0x00000002)) {
        output.writeInt32(2, sceneId_);
      }
      if (((bitField0_ & 0x00000004) == 0x00000004)) {
        output.writeInt32(3, locationId_);
      }
      if (((bitField0_ & 0x00000008) == 0x00000008)) {
        output.writeInt32(4, userType_);
      }
      if (((bitField0_ & 0x00000010) == 0x00000010)) {
        output.writeBytes(5, phoneBrand_);
      }
      for (int i = 0; i < timeStamp_.size(); i++) {
        output.writeUInt64(6, timeStamp_.get(i));
      }
      for (int i = 0; i < rssi_.size(); i++) {
        output.writeSInt32(7, rssi_.get(i));
      }
      getUnknownFields().writeTo(output);
    }

    private int memoizedSerializedSize = -1;
    public int getSerializedSize() {
      int size = memoizedSerializedSize;
      if (size != -1) return size;

      size = 0;
      if (((bitField0_ & 0x00000001) == 0x00000001)) {
        size += com.google.protobuf.CodedOutputStream
                .computeBytesSize(1, phoneMac_);
      }
      if (((bitField0_ & 0x00000002) == 0x00000002)) {
        size += com.google.protobuf.CodedOutputStream
                .computeInt32Size(2, sceneId_);
      }
      if (((bitField0_ & 0x00000004) == 0x00000004)) {
        size += com.google.protobuf.CodedOutputStream
                .computeInt32Size(3, locationId_);
      }
      if (((bitField0_ & 0x00000008) == 0x00000008)) {
        size += com.google.protobuf.CodedOutputStream
                .computeInt32Size(4, userType_);
      }
      if (((bitField0_ & 0x00000010) == 0x00000010)) {
        size += com.google.protobuf.CodedOutputStream
                .computeBytesSize(5, phoneBrand_);
      }
      {
        int dataSize = 0;
        for (int i = 0; i < timeStamp_.size(); i++) {
          dataSize += com.google.protobuf.CodedOutputStream
                  .computeUInt64SizeNoTag(timeStamp_.get(i));
        }
        size += dataSize;
        size += 1 * getTimeStampList().size();
      }
      {
        int dataSize = 0;
        for (int i = 0; i < rssi_.size(); i++) {
          dataSize += com.google.protobuf.CodedOutputStream
                  .computeSInt32SizeNoTag(rssi_.get(i));
        }
        size += dataSize;
        size += 1 * getRssiList().size();
      }
      size += getUnknownFields().getSerializedSize();
      memoizedSerializedSize = size;
      return size;
    }

    private static final long serialVersionUID = 0L;
    @Override
    protected Object writeReplace()
            throws java.io.ObjectStreamException {
      return super.writeReplace();
    }

    public static com.palmap.rssi.message.ShopStore.Visitor parseFrom(
            com.google.protobuf.ByteString data)
            throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }
    public static com.palmap.rssi.message.ShopStore.Visitor parseFrom(
            com.google.protobuf.ByteString data,
            com.google.protobuf.ExtensionRegistryLite extensionRegistry)
            throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }
    public static com.palmap.rssi.message.ShopStore.Visitor parseFrom(byte[] data)
            throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }
    public static com.palmap.rssi.message.ShopStore.Visitor parseFrom(
            byte[] data,
            com.google.protobuf.ExtensionRegistryLite extensionRegistry)
            throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }
    public static com.palmap.rssi.message.ShopStore.Visitor parseFrom(java.io.InputStream input)
            throws java.io.IOException {
      return PARSER.parseFrom(input);
    }
    public static com.palmap.rssi.message.ShopStore.Visitor parseFrom(
            java.io.InputStream input,
            com.google.protobuf.ExtensionRegistryLite extensionRegistry)
            throws java.io.IOException {
      return PARSER.parseFrom(input, extensionRegistry);
    }
    public static com.palmap.rssi.message.ShopStore.Visitor parseDelimitedFrom(java.io.InputStream input)
            throws java.io.IOException {
      return PARSER.parseDelimitedFrom(input);
    }
    public static com.palmap.rssi.message.ShopStore.Visitor parseDelimitedFrom(
            java.io.InputStream input,
            com.google.protobuf.ExtensionRegistryLite extensionRegistry)
            throws java.io.IOException {
      return PARSER.parseDelimitedFrom(input, extensionRegistry);
    }
    public static com.palmap.rssi.message.ShopStore.Visitor parseFrom(
            com.google.protobuf.CodedInputStream input)
            throws java.io.IOException {
      return PARSER.parseFrom(input);
    }
    public static com.palmap.rssi.message.ShopStore.Visitor parseFrom(
            com.google.protobuf.CodedInputStream input,
            com.google.protobuf.ExtensionRegistryLite extensionRegistry)
            throws java.io.IOException {
      return PARSER.parseFrom(input, extensionRegistry);
    }

    public static Builder newBuilder() { return Builder.create(); }
    public Builder newBuilderForType() { return newBuilder(); }
    public static Builder newBuilder(com.palmap.rssi.message.ShopStore.Visitor prototype) {
      return newBuilder().mergeFrom(prototype);
    }
    public Builder toBuilder() { return newBuilder(this); }

    @Override
    protected Builder newBuilderForType(
            com.google.protobuf.GeneratedMessage.BuilderParent parent) {
      Builder builder = new Builder(parent);
      return builder;
    }
    /**
     * Protobuf type {@code palmapmsg.Visitor}
     *
     * <pre>
     *rdd
     * </pre>
     */
    public static final class Builder extends
            com.google.protobuf.GeneratedMessage.Builder<Builder> implements
            // @@protoc_insertion_point(builder_implements:palmapmsg.Visitor)
            com.palmap.rssi.message.ShopStore.VisitorOrBuilder {
      public static final com.google.protobuf.Descriptors.Descriptor
      getDescriptor() {
        return com.palmap.rssi.message.ShopStore.internal_static_palmapmsg_Visitor_descriptor;
      }

      protected com.google.protobuf.GeneratedMessage.FieldAccessorTable
      internalGetFieldAccessorTable() {
        return com.palmap.rssi.message.ShopStore.internal_static_palmapmsg_Visitor_fieldAccessorTable
                .ensureFieldAccessorsInitialized(
                        com.palmap.rssi.message.ShopStore.Visitor.class, com.palmap.rssi.message.ShopStore.Visitor.Builder.class);
      }

      // Construct using com.palmap.rssi.message.ShopStore.Visitor.newBuilder()
      private Builder() {
        maybeForceBuilderInitialization();
      }

      private Builder(
              com.google.protobuf.GeneratedMessage.BuilderParent parent) {
        super(parent);
        maybeForceBuilderInitialization();
      }
      private void maybeForceBuilderInitialization() {
        if (com.google.protobuf.GeneratedMessage.alwaysUseFieldBuilders) {
        }
      }
      private static Builder create() {
        return new Builder();
      }

      public Builder clear() {
        super.clear();
        phoneMac_ = com.google.protobuf.ByteString.EMPTY;
        bitField0_ = (bitField0_ & ~0x00000001);
        sceneId_ = 0;
        bitField0_ = (bitField0_ & ~0x00000002);
        locationId_ = 0;
        bitField0_ = (bitField0_ & ~0x00000004);
        userType_ = 0;
        bitField0_ = (bitField0_ & ~0x00000008);
        phoneBrand_ = com.google.protobuf.ByteString.EMPTY;
        bitField0_ = (bitField0_ & ~0x00000010);
        timeStamp_ = java.util.Collections.emptyList();
        bitField0_ = (bitField0_ & ~0x00000020);
        rssi_ = java.util.Collections.emptyList();
        bitField0_ = (bitField0_ & ~0x00000040);
        return this;
      }

      public Builder clone() {
        return create().mergeFrom(buildPartial());
      }

      public com.google.protobuf.Descriptors.Descriptor
      getDescriptorForType() {
        return com.palmap.rssi.message.ShopStore.internal_static_palmapmsg_Visitor_descriptor;
      }

      public com.palmap.rssi.message.ShopStore.Visitor getDefaultInstanceForType() {
        return com.palmap.rssi.message.ShopStore.Visitor.getDefaultInstance();
      }

      public com.palmap.rssi.message.ShopStore.Visitor build() {
        com.palmap.rssi.message.ShopStore.Visitor result = buildPartial();
        if (!result.isInitialized()) {
          throw newUninitializedMessageException(result);
        }
        return result;
      }

      public com.palmap.rssi.message.ShopStore.Visitor buildPartial() {
        com.palmap.rssi.message.ShopStore.Visitor result = new com.palmap.rssi.message.ShopStore.Visitor(this);
        int from_bitField0_ = bitField0_;
        int to_bitField0_ = 0;
        if (((from_bitField0_ & 0x00000001) == 0x00000001)) {
          to_bitField0_ |= 0x00000001;
        }
        result.phoneMac_ = phoneMac_;
        if (((from_bitField0_ & 0x00000002) == 0x00000002)) {
          to_bitField0_ |= 0x00000002;
        }
        result.sceneId_ = sceneId_;
        if (((from_bitField0_ & 0x00000004) == 0x00000004)) {
          to_bitField0_ |= 0x00000004;
        }
        result.locationId_ = locationId_;
        if (((from_bitField0_ & 0x00000008) == 0x00000008)) {
          to_bitField0_ |= 0x00000008;
        }
        result.userType_ = userType_;
        if (((from_bitField0_ & 0x00000010) == 0x00000010)) {
          to_bitField0_ |= 0x00000010;
        }
        result.phoneBrand_ = phoneBrand_;
        if (((bitField0_ & 0x00000020) == 0x00000020)) {
          timeStamp_ = java.util.Collections.unmodifiableList(timeStamp_);
          bitField0_ = (bitField0_ & ~0x00000020);
        }
        result.timeStamp_ = timeStamp_;
        if (((bitField0_ & 0x00000040) == 0x00000040)) {
          rssi_ = java.util.Collections.unmodifiableList(rssi_);
          bitField0_ = (bitField0_ & ~0x00000040);
        }
        result.rssi_ = rssi_;
        result.bitField0_ = to_bitField0_;
        onBuilt();
        return result;
      }

      public Builder mergeFrom(com.google.protobuf.Message other) {
        if (other instanceof com.palmap.rssi.message.ShopStore.Visitor) {
          return mergeFrom((com.palmap.rssi.message.ShopStore.Visitor)other);
        } else {
          super.mergeFrom(other);
          return this;
        }
      }

      public Builder mergeFrom(com.palmap.rssi.message.ShopStore.Visitor other) {
        if (other == com.palmap.rssi.message.ShopStore.Visitor.getDefaultInstance()) return this;
        if (other.hasPhoneMac()) {
          setPhoneMac(other.getPhoneMac());
        }
        if (other.hasSceneId()) {
          setSceneId(other.getSceneId());
        }
        if (other.hasLocationId()) {
          setLocationId(other.getLocationId());
        }
        if (other.hasUserType()) {
          setUserType(other.getUserType());
        }
        if (other.hasPhoneBrand()) {
          setPhoneBrand(other.getPhoneBrand());
        }
        if (!other.timeStamp_.isEmpty()) {
          if (timeStamp_.isEmpty()) {
            timeStamp_ = other.timeStamp_;
            bitField0_ = (bitField0_ & ~0x00000020);
          } else {
            ensureTimeStampIsMutable();
            timeStamp_.addAll(other.timeStamp_);
          }
          onChanged();
        }
        if (!other.rssi_.isEmpty()) {
          if (rssi_.isEmpty()) {
            rssi_ = other.rssi_;
            bitField0_ = (bitField0_ & ~0x00000040);
          } else {
            ensureRssiIsMutable();
            rssi_.addAll(other.rssi_);
          }
          onChanged();
        }
        this.mergeUnknownFields(other.getUnknownFields());
        return this;
      }

      public final boolean isInitialized() {
        return true;
      }

      public Builder mergeFrom(
              com.google.protobuf.CodedInputStream input,
              com.google.protobuf.ExtensionRegistryLite extensionRegistry)
              throws java.io.IOException {
        com.palmap.rssi.message.ShopStore.Visitor parsedMessage = null;
        try {
          parsedMessage = PARSER.parsePartialFrom(input, extensionRegistry);
        } catch (com.google.protobuf.InvalidProtocolBufferException e) {
          parsedMessage = (com.palmap.rssi.message.ShopStore.Visitor) e.getUnfinishedMessage();
          throw e;
        } finally {
          if (parsedMessage != null) {
            mergeFrom(parsedMessage);
          }
        }
        return this;
      }
      private int bitField0_;

      private com.google.protobuf.ByteString phoneMac_ = com.google.protobuf.ByteString.EMPTY;
      /**
       * <code>optional bytes phone_mac = 1;</code>
       *
       * <pre>
       * </pre>
       */
      public boolean hasPhoneMac() {
        return ((bitField0_ & 0x00000001) == 0x00000001);
      }
      /**
       * <code>optional bytes phone_mac = 1;</code>
       *
       * <pre>
       * </pre>
       */
      public com.google.protobuf.ByteString getPhoneMac() {
        return phoneMac_;
      }
      /**
       * <code>optional bytes phone_mac = 1;</code>
       *
       * <pre>
       * </pre>
       */
      public Builder setPhoneMac(com.google.protobuf.ByteString value) {
        if (value == null) {
          throw new NullPointerException();
        }
        bitField0_ |= 0x00000001;
        phoneMac_ = value;
        onChanged();
        return this;
      }
      /**
       * <code>optional bytes phone_mac = 1;</code>
       *
       * </pre>
       */
      public Builder clearPhoneMac() {
        bitField0_ = (bitField0_ & ~0x00000001);
        phoneMac_ = getDefaultInstance().getPhoneMac();
        onChanged();
        return this;
      }

      private int sceneId_ ;
      /**
       * <code>optional int32 scene_id = 2;</code>
       *
       * <pre>
       * ????id
       * </pre>
       */
      public boolean hasSceneId() {
        return ((bitField0_ & 0x00000002) == 0x00000002);
      }
      /**
       * <code>optional int32 scene_id = 2;</code>
       *
       * <pre>
       * </pre>
       */
      public int getSceneId() {
        return sceneId_;
      }
      /**
       * <code>optional int32 scene_id = 2;</code>
       *
       * </pre>
       */
      public Builder setSceneId(int value) {
        bitField0_ |= 0x00000002;
        sceneId_ = value;
        onChanged();
        return this;
      }
      /**
       * <code>optional int32 scene_id = 2;</code>
       *
       * <pre>
       */
      public Builder clearSceneId() {
        bitField0_ = (bitField0_ & ~0x00000002);
        sceneId_ = 0;
        onChanged();
        return this;
      }

      private int locationId_ ;
      /**
       * <code>optional int32 location_id = 3;</code>
       *
       * <pre>
       * </pre>
       */
      public boolean hasLocationId() {
        return ((bitField0_ & 0x00000004) == 0x00000004);
      }
      /**
       * <code>optional int32 location_id = 3;</code>
       *
       * </pre>
       */
      public int getLocationId() {
        return locationId_;
      }
      /**
       * <code>optional int32 location_id = 3;</code>
       *
       * <pre>
       * </pre>
       */
      public Builder setLocationId(int value) {
        bitField0_ |= 0x00000004;
        locationId_ = value;
        onChanged();
        return this;
      }
      /**
       * <code>optional int32 location_id = 3;</code>
       *
       * </pre>
       */
      public Builder clearLocationId() {
        bitField0_ = (bitField0_ & ~0x00000004);
        locationId_ = 0;
        onChanged();
        return this;
      }

      private int userType_ ;
      /**
       * <code>optional int32 user_type = 4;</code>
       *
       * <pre>
       * </pre>
       */
      public boolean hasUserType() {
        return ((bitField0_ & 0x00000008) == 0x00000008);
      }
      /**
       * <code>optional int32 user_type = 4;</code>
       *
       * <pre>
       * </pre>
       */
      public int getUserType() {
        return userType_;
      }
      /**
       * <code>optional int32 user_type = 4;</code>
       *
       * <pre>
       * </pre>
       */
      public Builder setUserType(int value) {
        bitField0_ |= 0x00000008;
        userType_ = value;
        onChanged();
        return this;
      }
      /**
       * <code>optional int32 user_type = 4;</code>
       *
       * <pre>
       * </pre>
       */
      public Builder clearUserType() {
        bitField0_ = (bitField0_ & ~0x00000008);
        userType_ = 0;
        onChanged();
        return this;
      }

      private com.google.protobuf.ByteString phoneBrand_ = com.google.protobuf.ByteString.EMPTY;
      /**
       * <code>optional bytes phone_brand = 5;</code>
       *
       * <pre>
       * </pre>
       */
      public boolean hasPhoneBrand() {
        return ((bitField0_ & 0x00000010) == 0x00000010);
      }
      /**
       * <code>optional bytes phone_brand = 5;</code>
       *
       * <pre>
       * </pre>
       */
      public com.google.protobuf.ByteString getPhoneBrand() {
        return phoneBrand_;
      }
      /**
       * <code>optional bytes phone_brand = 5;</code>
       *
       * <pre>
       * </pre>
       */
      public Builder setPhoneBrand(com.google.protobuf.ByteString value) {
        if (value == null) {
          throw new NullPointerException();
        }
        bitField0_ |= 0x00000010;
        phoneBrand_ = value;
        onChanged();
        return this;
      }
      /**
       * <code>optional bytes phone_brand = 5;</code>
       *
       * <pre>
       * </pre>
       */
      public Builder clearPhoneBrand() {
        bitField0_ = (bitField0_ & ~0x00000010);
        phoneBrand_ = getDefaultInstance().getPhoneBrand();
        onChanged();
        return this;
      }

      private java.util.List<Long> timeStamp_ = java.util.Collections.emptyList();
      private void ensureTimeStampIsMutable() {
        if (!((bitField0_ & 0x00000020) == 0x00000020)) {
          timeStamp_ = new java.util.ArrayList<Long>(timeStamp_);
          bitField0_ |= 0x00000020;
        }
      }
      /**
       * <code>repeated uint64 time_stamp = 6;</code>
       *
       * <pre>
       * </pre>
       */
      public java.util.List<Long>
      getTimeStampList() {
        return java.util.Collections.unmodifiableList(timeStamp_);
      }
      /**
       * <code>repeated uint64 time_stamp = 6;</code>
       *
       * <pre>
       * </pre>
       */
      public int getTimeStampCount() {
        return timeStamp_.size();
      }
      /**
       * <code>repeated uint64 time_stamp = 6;</code>
       *
       * <pre>
       * </pre>
       */
      public long getTimeStamp(int index) {
        return timeStamp_.get(index);
      }
      /**
       * <code>repeated uint64 time_stamp = 6;</code>
       *
       * <pre>
       * </pre>
       */
      public Builder setTimeStamp(
              int index, long value) {
        ensureTimeStampIsMutable();
        timeStamp_.set(index, value);
        onChanged();
        return this;
      }
      /**
       * <code>repeated uint64 time_stamp = 6;</code>
       *
       * <pre>
       * </pre>
       */
      public Builder addTimeStamp(long value) {
        ensureTimeStampIsMutable();
        timeStamp_.add(value);
        onChanged();
        return this;
      }
      /**
       * <code>repeated uint64 time_stamp = 6;</code>
       *
       * <pre>
       * </pre>
       */
      public Builder addAllTimeStamp(
              Iterable<? extends Long> values) {
        ensureTimeStampIsMutable();
        com.google.protobuf.AbstractMessageLite.Builder.addAll(
                values, timeStamp_);
        onChanged();
        return this;
      }
      /**
       * <code>repeated uint64 time_stamp = 6;</code>
       *
       * <pre>
       * </pre>
       */
      public Builder clearTimeStamp() {
        timeStamp_ = java.util.Collections.emptyList();
        bitField0_ = (bitField0_ & ~0x00000020);
        onChanged();
        return this;
      }

      private java.util.List<Integer> rssi_ = java.util.Collections.emptyList();
      private void ensureRssiIsMutable() {
        if (!((bitField0_ & 0x00000040) == 0x00000040)) {
          rssi_ = new java.util.ArrayList<Integer>(rssi_);
          bitField0_ |= 0x00000040;
        }
      }
      /**
       * <code>repeated sint32 rssi = 7;</code>
       *
       * <pre>
       * </pre>
       */
      public java.util.List<Integer>
      getRssiList() {
        return java.util.Collections.unmodifiableList(rssi_);
      }
      /**
       * <code>repeated sint32 rssi = 7;</code>
       *
       * <pre>
       * </pre>
       */
      public int getRssiCount() {
        return rssi_.size();
      }
      /**
       * <code>repeated sint32 rssi = 7;</code>
       *
       * <pre>
       * </pre>
       */
      public int getRssi(int index) {
        return rssi_.get(index);
      }
      /**
       * <code>repeated sint32 rssi = 7;</code>
       *
       * <pre>
       * </pre>
       */
      public Builder setRssi(
              int index, int value) {
        ensureRssiIsMutable();
        rssi_.set(index, value);
        onChanged();
        return this;
      }
      /**
       * <code>repeated sint32 rssi = 7;</code>
       *
       * <pre>
       * </pre>
       */
      public Builder addRssi(int value) {
        ensureRssiIsMutable();
        rssi_.add(value);
        onChanged();
        return this;
      }
      /**
       * <code>repeated sint32 rssi = 7;</code>
       *
       * <pre>
       * </pre>
       */
      public Builder addAllRssi(
              Iterable<? extends Integer> values) {
        ensureRssiIsMutable();
        com.google.protobuf.AbstractMessageLite.Builder.addAll(
                values, rssi_);
        onChanged();
        return this;
      }
      /**
       * <code>repeated sint32 rssi = 7;</code>
       *
       * <pre>
       * </pre>
       */
      public Builder clearRssi() {
        rssi_ = java.util.Collections.emptyList();
        bitField0_ = (bitField0_ & ~0x00000040);
        onChanged();
        return this;
      }

      // @@protoc_insertion_point(builder_scope:palmapmsg.Visitor)
    }

    static {
      defaultInstance = new Visitor(true);
      defaultInstance.initFields();
    }

    // @@protoc_insertion_point(class_scope:palmapmsg.Visitor)
  }

  private static final com.google.protobuf.Descriptors.Descriptor
          internal_static_palmapmsg_Visitor_descriptor;
  private static
  com.google.protobuf.GeneratedMessage.FieldAccessorTable
          internal_static_palmapmsg_Visitor_fieldAccessorTable;

  public static com.google.protobuf.Descriptors.FileDescriptor
  getDescriptor() {
    return descriptor;
  }
  private static com.google.protobuf.Descriptors.FileDescriptor
          descriptor;
  static {
    String[] descriptorData = {
            "\n\017ShopStore.proto\022\tpalmapmsg\"\215\001\n\007Visitor" +
                    "\022\021\n\tphone_mac\030\001 \001(\014\022\020\n\010scene_id\030\002 \001(\005\022\023\n" +
                    "\013location_id\030\003 \001(\005\022\021\n\tuser_type\030\004 \001(\005\022\023\n" +
                    "\013phone_brand\030\005 \001(\014\022\022\n\ntime_stamp\030\006 \003(\004\022\014" +
                    "\n\004rssi\030\007 \003(\021B\031\n\027com.palmap.rssi.message"
    };
    com.google.protobuf.Descriptors.FileDescriptor.InternalDescriptorAssigner assigner =
            new com.google.protobuf.Descriptors.FileDescriptor.    InternalDescriptorAssigner() {
              public com.google.protobuf.ExtensionRegistry assignDescriptors(
                      com.google.protobuf.Descriptors.FileDescriptor root) {
                descriptor = root;
                return null;
              }
            };
    com.google.protobuf.Descriptors.FileDescriptor
            .internalBuildGeneratedFileFrom(descriptorData,
                    new com.google.protobuf.Descriptors.FileDescriptor[] {
                    }, assigner);
    internal_static_palmapmsg_Visitor_descriptor =
            getDescriptor().getMessageTypes().get(0);
    internal_static_palmapmsg_Visitor_fieldAccessorTable = new
            com.google.protobuf.GeneratedMessage.FieldAccessorTable(
            internal_static_palmapmsg_Visitor_descriptor,
            new String[] { "PhoneMac", "SceneId", "LocationId", "UserType", "PhoneBrand", "TimeStamp", "Rssi", });
  }

  // @@protoc_insertion_point(outer_class_scope)
}
