/*
 * Copyright 2022 TiDB Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.tidb.bigdata.telemetry;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import oshi.SystemInfo;
import oshi.hardware.CentralProcessor;
import oshi.hardware.CentralProcessor.ProcessorIdentifier;
import oshi.hardware.HWDiskStore;
import oshi.hardware.HardwareAbstractionLayer;
import oshi.software.os.OperatingSystem;

/** SystemInfoUtil is used to get system and hardware information. */
public class SystemInfoUtil {

  private final HardwareAbstractionLayer hal;
  private final OperatingSystem operatingSystem;
  private final CentralProcessor processor;
  private final ProcessorIdentifier processorIdentifier;

  public SystemInfoUtil() {
    SystemInfo si = new SystemInfo();
    this.hal = si.getHardware();
    this.operatingSystem = si.getOperatingSystem();
    this.processor = hal.getProcessor();
    this.processorIdentifier = processor.getProcessorIdentifier();
  }

  /**
   * Get the Operating System family.
   *
   * @return the system family
   */
  public String getOsFamily() {
    return operatingSystem.getFamily();
  }

  /**
   * Get Operating System version information.
   *
   * @return version information
   */
  public String getOsVersion() {
    return operatingSystem.getVersionInfo().toString();
  }

  /**
   * Name, eg. Intel(R) Core(TM)2 Duo CPU T7300 @ 2.00GHz
   *
   * @return Processor name.
   */
  public String getCpuName() {
    return processorIdentifier.getName();
  }

  /**
   * Get the number of logical CPUs available for processing.
   *
   * @return The number of logical CPUs available.
   */
  public int getCpuLogicalCores() {
    return processor.getLogicalProcessorCount();
  }

  /**
   * Get the number of physical CPUs/cores available for processing.
   *
   * @return The number of physical CPUs available.
   */
  public int getCpuPhysicalCore() {
    return processor.getPhysicalProcessorCount();
  }

  public Map<String, Object> getCpu() {
    Map<String, Object> cpu = new HashMap<>();
    cpu.put("model", this.getCpuName());
    cpu.put("logicalCores", this.getCpuLogicalCores());
    cpu.put("physicalCores", this.getCpuPhysicalCore());
    return cpu;
  }

  /**
   * Get memory size.
   *
   * @return The free and used size.
   */
  public String getMemoryInfo() {
    return hal.getMemory().toString();
  }

  /**
   * Get disks' size and type
   *
   * @return A List of {disk name, disk size}
   */
  public List<Map<String, Object>> getDisks() {
    List<Map<String, Object>> disks = new ArrayList<>();
    for (HWDiskStore i : hal.getDiskStores()) {
      Map<String, Object> temp = new HashMap<>();
      temp.put("name", i.getName());
      temp.put("size", i.getSize());
      disks.add(temp);
    }
    return disks;
  }
}
