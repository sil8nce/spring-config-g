package cn.net.wanji.iot.customer.dhsignal;

import cn.net.wanji.iot.components.client.Client;
import cn.net.wanji.iot.components.common.ComponentsConst;
import cn.net.wanji.iot.components.utils.AsyncTaskArgc;
import cn.net.wanji.iot.components.utils.ComponentsUtils;
import cn.net.wanji.iot.customer.dhsignal.signal.SignalData;
import cn.net.wanji.iot.customer.dhsignal.signal.TempOriginalSignal;
import cn.net.wanji.iot.flow.AbstractFlowNode;
import cn.net.wanji.iot.flow.NodeType;
import com.netsdk.lib.LibraryLoad;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author hanchaoyong
 * @version 1.0
 * @className DHClient
 * @description TODO
 * @date 2022-09-16 11:09
 **/
public class DHClient extends AbstractFlowNode<Map<String, Object>, Object>
    implements Client {
  private static final Logger logger = LoggerFactory.getLogger(DHClient.class);
  private RtscEvent rtscEvent;
  private DHClientProperties clientProperties;
  private AtomicBoolean isAlive = new AtomicBoolean(false);
  private int interval;

  public DHClient(DHClientProperties clientProperties) {
    this.clientProperties = clientProperties;
  }

  /**
   * 客户端初始化记录
   */
  private boolean clientInit;

  @Override
  public void onInit() throws Exception {
    // 设置动态库位置
    if (StringUtils.isNoneBlank(clientProperties.getDynamicLibPath())) {
      LibraryLoad.setExtractPath(clientProperties.getDynamicLibPath());
    }
    rtscEvent = new RtscEvent();
  }

  @Override
  public void onStart() {
    new AsyncTaskArgc<Boolean, DHClient>(this) {
      @Override
      public Boolean call() throws Exception {
        rtscEvent.startSignal(clientProperties, this.getO());
        return true;
      }
    }.execute();
  }

  @Override
  public Object onInput(Map<String, Object> in) throws Exception {
    Iterator<Map.Entry<String, Object>> iterator = in.entrySet().iterator();
    while (iterator.hasNext()) {
      Map.Entry<String, Object> next = iterator.next();
      return ComponentsUtils.createResult(
          executeSignalMethod(next.getKey(), next.getValue()));
    }
    return null;
  }

  private Object executeSignalMethod(String k, Object v) {
    switch (k) {
      case "sendTempScheme":
        // 下发临时方案
        return rtscEvent.sendTempScheme(((TempOriginalSignal) v));
      case "sendDefaultScheme":
        // 下发默认方案
        return rtscEvent.sendDefaultScheme((SignalData) v);
      case "getTempSchemeConfig":
        // 获取临时方案配置
        return rtscEvent.getTempSchemeConfig();
      case "getDefaultSchemeConfig":
        // 获取默认方案配置
        return rtscEvent.getDefaultSchemeConfig();
      case "getSignalWorkingInfo":
        return rtscEvent.getSignalWorkingInfo((Integer) v);
      default:
        return "Nothing matches!";
    }
  }

  @Override
  public NodeType getType() {
    return null;
  }

  @Override
  public String getName() {
    return null;
  }

  @Override
  public void onDestroy() throws Exception {
    new Thread(() -> {
      rtscEvent.stopListen(clientProperties);
      rtscEvent.logOut(clientProperties);
      rtscEvent = null;
    }).start();

  }

  @Override
  public void onShutdown() throws Exception {
    // rtscEvent = null;
  }

  @Override
  public InetSocketAddress address() {
    return new InetSocketAddress(clientProperties.getHost(),
        clientProperties.getPort());
  }

  @Override
  public void disconnect() {

  }

  @Override
  public boolean isAlive() {
    return isAlive.get();
  }

  @Override
  public void reconnection() {
    if (interval < ComponentsConst.CONNECT_MAX_INTERVAL) {
      interval += 5;
    }
    try {
      Thread.sleep(interval * 1000);
    } catch (InterruptedException e) {
      logger.error("dh client [{}][{}]reconnection error!",
          clientProperties.getHost(), clientProperties.getPort());
    }
    rtscEvent.startSignal(clientProperties, this);
  }

  public void setIsAlive(boolean isAlive) {
    this.isAlive.set(isAlive);
  }

}
