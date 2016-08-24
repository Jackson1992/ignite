/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.marshaller;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.LocalGridName;
import org.apache.ignite.internal.IgnitionEx;
import org.apache.ignite.internal.client.marshaller.GridClientMarshaller;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;

/**
 * Util class that sets and discards thread local
 * ignite grid name in {@link IgnitionEx} class.
 */
public final class MarshallerUtils {
    /**
     *
     */
    private MarshallerUtils() {
    }

    /**
     * Marshal object to stream and set grid name thread local.
     *
     * @param marshaller Marshaller.
     * @param obj Object to marshal.
     * @param out Output stream.
     * @param gridName Grid name.
     * @throws IgniteCheckedException If fail.
     */
    public static void marshal(final Marshaller marshaller, final @Nullable Object obj,
        final OutputStream out, final String gridName) throws IgniteCheckedException {
        final LocalGridName gridNameTl = gridName();

        final String gridNameStr = gridNameTl.getGridName();
        final boolean init = gridNameTl.isSet();

        try {
            gridNameTl.setGridName(true, gridName);

            marshaller.marshal(obj, out);
        } finally {
            gridNameTl.setGridName(init, gridNameStr);
        }
    }

    /**
     * Marshal object and set grid name thread local.
     *
     * @param marshaller Marshaller.
     * @param obj Object to marshal.
     * @param gridName Grid name.
     * @return Binary data.
     * @throws IgniteCheckedException If fail.
     */
    public static byte[] marshal(final Marshaller marshaller, @Nullable Object obj,
        final String gridName) throws IgniteCheckedException {
        final LocalGridName gridNameTl = gridName();

        final String gridNameStr = gridNameTl.getGridName();
        final boolean init = gridNameTl.isSet();

        try {
            gridNameTl.setGridName(true, gridName);

            return marshaller.marshal(obj);
        }
        finally {
            gridNameTl.setGridName(init, gridNameStr);
        }
    }

    /**
     * Marshal object.
     * <p>Use only when grid name is not available, f.e. in tests.</p>
     *
     * @param marshaller Marshaller.
     * @param obj Object to marshal.
     * @return Binary data.
     * @throws IgniteCheckedException If fail.
     */
    public static byte[] marshal(final Marshaller marshaller, final @Nullable Object obj) throws IgniteCheckedException {
        // This method used to keep marshaller usages in one place.
        return marshaller.marshal(obj);
    }

    /**
     * Unmarshal object from stream and set grid name thread local.
     *
     * @param marshaller Marshaller.
     * @param in Input stream.
     * @param clsLdr Class loader.
     * @param gridName Grid name.
     * @param <T> Target type.
     * @return Deserialized object.
     * @throws IgniteCheckedException
     */
    public static <T> T unmarshal(final Marshaller marshaller, InputStream in, @Nullable ClassLoader clsLdr,
        final String gridName) throws IgniteCheckedException {
        final LocalGridName gridNameTl = gridName();

        final String gridNameStr = gridNameTl.getGridName();
        final boolean init = gridNameTl.isSet();

        try {
            gridNameTl.setGridName(true, gridName);

            return marshaller.unmarshal(in, clsLdr);
        } finally {
            gridNameTl.setGridName(init, gridNameStr);
        }
    }

    /**
     * Unmarshal object.
     * <p>Use only when grid name is not available, f.e. in tests.</p>
     *
     * @param marshaller Marshaller.
     * @param arr Bianry data.
     * @param clsLdr Class loader.
     * @param <T> Target type.
     * @return Unmarshalled object.
     * @throws IgniteCheckedException If fail.
     */
    public static <T> T unmarshal(final Marshaller marshaller, byte[] arr, @Nullable ClassLoader clsLdr)
        throws IgniteCheckedException {
        // This method used to keep marshaller usages in one place.
        return marshaller.unmarshal(arr, clsLdr);
    }

    /**
     * Unmarshal object and set grid name thread local.
     *
     * @param marshaller Marshaller.
     * @param arr Bianry data.
     * @param clsLdr Class loader.
     * @param gridName Grid name.
     * @param <T> Target type
     * @return Deserialized object.
     * @throws IgniteCheckedException If fail.
     */
    public static <T> T unmarshal(final Marshaller marshaller, byte[] arr, @Nullable ClassLoader clsLdr,
        final String gridName) throws IgniteCheckedException {
        final LocalGridName gridNameTl = gridName();

        final String gridNameStr = gridNameTl.getGridName();
        final boolean init = gridNameTl.isSet();

        try {
            gridNameTl.setGridName(true, gridName);

            return marshaller.unmarshal(arr, clsLdr);
        } finally {
            gridNameTl.setGridName(init, gridNameStr);
        }
    }

    /**
     * Marshal and unmarshal object.
     *
     * @param marshaller Marshaller.
     * @param obj Object to clone.
     * @param clsLdr Class loader.
     * @param gridName Grid name.
     * @param <T> Target type.
     * @return Deserialized value.
     * @throws IgniteCheckedException If fail.
     */
    public static <T> T clone(final Marshaller marshaller, T obj, @Nullable ClassLoader clsLdr,
        final String gridName) throws IgniteCheckedException {
        final LocalGridName gridNameTl = gridName();

        final String gridNameStr = gridNameTl.getGridName();
        final boolean init = gridNameTl.isSet();

        try {
            gridNameTl.setGridName(true, gridName);

            return marshaller.unmarshal(marshaller.marshal(obj), clsLdr);
        } finally {
            gridNameTl.setGridName(init, gridNameStr);
        }
    }

    /**
     * Marshal object and set grid name thread local.
     *
     * @param gridMarshaller Grid marshaller.
     * @param obj Object to marshal.
     * @param off Offset.
     * @param gridName Grid name.
     * @return Serialized data.
     * @throws IOException If fail.
     */
    public static ByteBuffer marshal(GridClientMarshaller gridMarshaller, Object obj, int off,
        String gridName) throws IOException {
        final LocalGridName gridNameTl = gridName();

        final String gridNameStr = gridNameTl.getGridName();
        final boolean init = gridNameTl.isSet();

        try {
            gridNameTl.setGridName(true, gridName);

            return gridMarshaller.marshal(obj, off);
        } finally {
            gridNameTl.setGridName(init, gridNameStr);
        }
    }

    /**
     * Unmarshal object and set grid name thread local.
     *
     * @param gridMarshaller Grid marshaller.
     * @param bytes Binary data.
     * @param gridName Grid name.
     * @param <T> Target type.
     * @return Deserialized value.
     * @throws IOException If fail.
     */
    public static <T> T unmarshal(GridClientMarshaller gridMarshaller, byte[] bytes,
        String gridName) throws IOException {
        final LocalGridName gridNameTl = gridName();

        final String gridNameStr = gridNameTl.getGridName();
        final boolean init = gridNameTl.isSet();

        try {
            gridNameTl.setGridName(true, gridName);

            return gridMarshaller.unmarshal(bytes);
        } finally {
            gridNameTl.setGridName(init, gridNameStr);
        }
    }

    /**
     * @return Grid name thread local.
     */
    private static LocalGridName gridName() {
        return IgnitionEx.gridNameThreadLocal();
    }
}
