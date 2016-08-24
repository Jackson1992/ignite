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
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgnitionEx;
import org.apache.ignite.internal.LocalGridName;
import org.jetbrains.annotations.Nullable;

import java.io.InputStream;
import java.io.OutputStream;

/**
 * Utility marshaller methods.
 */
public final class MarshallerUtils {
    /**
     * Marshal object with provided node name.
     *
     * @param name Grid name.
     * @param marsh Marshaller.
     * @param obj Object to marshal.
     * @return Result.
     * @throws IgniteCheckedException If failed.
     */
    public static byte[] marshal(String name, Marshaller marsh, @Nullable Object obj) throws IgniteCheckedException {
        final LocalGridName gridNameTl = gridName();

        final String gridNameStr = gridNameTl.getGridName();
        final boolean init = gridNameTl.isSet();

        try {
            gridNameTl.setGridName(true, name);

            return marsh.marshal(obj);
        }
        finally {
            gridNameTl.setGridName(init, gridNameStr);
        }
    }

    /**
     * Marshal object to stream and set grid name thread local.
     *
     * @param name Grid name.
     * @param marshaller Marshaller.
     * @param obj Object to marshal.
     * @param out Output stream.
     * @throws IgniteCheckedException If fail.
     */
    public static void marshal(final String name, final Marshaller marshaller, final @Nullable Object obj,
        final OutputStream out) throws IgniteCheckedException {
        final LocalGridName gridNameTl = gridName();

        final String gridNameStr = gridNameTl.getGridName();
        final boolean init = gridNameTl.isSet();

        try {
            gridNameTl.setGridName(true, name);

            marshaller.marshal(obj, out);
        } finally {
            gridNameTl.setGridName(init, gridNameStr);
        }
    }

    /**
     * Marshal object with node name taken from provided kernal context.
     *
     * @param ctx Kernal context.
     * @param obj Object to marshal.
     * @return Result.
     * @throws IgniteCheckedException If failed.
     */
    public static byte[] marshal(GridKernalContext ctx, @Nullable Object obj) throws IgniteCheckedException {
        return marshal(ctx.gridName(), ctx.config().getMarshaller(), obj);
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
     * @throws IgniteCheckedException If failed.
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
     * @param <T> Target type.
     * @param gridName Grid name.
     * @param marshaller Marshaller.
     * @param obj Object to clone.
     * @param clsLdr Class loader.
     * @return Deserialized value.
     * @throws IgniteCheckedException If fail.
     */
    public static <T> T marshalUnmarshal(final String gridName, final Marshaller marshaller, T obj,
        @Nullable ClassLoader clsLdr) throws IgniteCheckedException {
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
     * @return Grid name thread local.
     */
    private static LocalGridName gridName() {
        return IgnitionEx.gridNameThreadLocal();
    }

    /**
     * Private constructor.
     */
    private MarshallerUtils() {
        // No-op.
    }
}
