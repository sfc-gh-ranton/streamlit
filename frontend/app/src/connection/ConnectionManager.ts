/**
 * Copyright (c) Streamlit Inc. (2018-2022) Snowflake Inc. (2022-2024)
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

import { ReactNode } from "react"

import url from "url"
import * as console from "node:console"

import {
  BackMsg,
  BaseUriParts,
  ensureError,
  ForwardMsg,
  getPossibleBaseUris,
  getReportObject,
  IHostConfigResponse,
  IS_SHARED_REPORT,
  logError,
  SessionInfo,
  StaticManifest,
  StreamlitEndpoints,
} from "@streamlit/lib"

import { ConnectionState } from "./ConnectionState"
import { StaticConnection } from "./StaticConnection"
import { WebsocketConnection } from "./WebsocketConnection"

// TODO: import {Promise} from "cypress/types/cy-bluebird";
// TODO: import * as url from "node:url";

/**
 * When the websocket connection retries this many times, we show a dialog
 * letting the user know we're having problems connecting. This happens
 * after about 15 seconds as, before the 6th retry, we've set timeouts for
 * a total of approximately 0.5 + 1 + 2 + 4 + 8 = 15.5 seconds (+/- some
 * due to jitter).
 */
const RETRY_COUNT_FOR_WARNING = 6

export interface ConnectionManagerProps {
  /** The app's SessionInfo instance */
  sessionInfo: SessionInfo

  /** The app's StreamlitEndpoints instance */
  endpoints: StreamlitEndpoints

  /**
   * Function to be called when we receive a message from the server.
   */
  onMessage: (message: ForwardMsg) => void

  /**
   * Function to be called when the connection errors out.
   */
  onConnectionError: (errNode: ReactNode) => void

  /**
   * Called when our ConnectionState is changed.
   */
  connectionStateChanged: (connectionState: ConnectionState) => void

  /**
   * Function to get the auth token set by the host of this app (if in a
   * relevant deployment scenario).
   */
  claimHostAuthToken: () => Promise<string | undefined>

  /**
   * Function to clear the withHostCommunication hoc's auth token. This should
   * be called after the promise returned by claimHostAuthToken successfully
   * resolves.
   */
  resetHostAuthToken: () => void

  /**
   * Function to set the host config for this app (if in a relevant deployment
   * scenario).
   */
  onHostConfigResp: (resp: IHostConfigResponse) => void
}
export class ConnectionManager {
  private readonly props: ConnectionManagerProps

  private connection?: WebsocketConnection | StaticConnection

  private connectionState: ConnectionState = ConnectionState.INITIAL

  constructor(props: ConnectionManagerProps) {
    // TODO: cleanup
    console.log(`You called constructor with ${JSON.stringify(props)}`)
    this.props = props

    const connectPromise = this.connect()
    connectPromise.then((value: any) => {
      console.log(`connect finished: ${value}`)
    })
    return this
  }

  public disconnect(): void {
    console.log("You disconnected.") // TODO: cleanup
    this.connection?.disconnect()
  }

  private setConnectionState = (
    connectionState: ConnectionState,
    errMsg?: string
  ): void => {
    console.log(`You called setConnectionState with ${connectionState}`)
    if (this.connectionState !== connectionState) {
      this.connectionState = connectionState
      this.props.connectionStateChanged(connectionState)
    }

    if (errMsg) {
      this.props.onConnectionError(errMsg)
    }
  }

  private async connect(): Promise<void> {
    console.log("You called connect.") // TODO: cleanup
    try {
      if (IS_SHARED_REPORT) {
        const { query } = url.parse(window.location.href, true)
        const scriptRunId = query.id as string
        console.log(
          `connect to shared report based on manifest, scriptRunId: ${scriptRunId}`
        )
        this.connection = await this.connectBasedOnManifest(scriptRunId)
      } else {
        console.log("Connecting to running server") // TODO:
        // TODO: "TS80007: 'await' has no effect on the type of this expression."
        this.connection = await this.connectToRunningServer()
      }
    } catch (e) {
      const err = ensureError(e)

      logError(err.message)
      this.setConnectionState(
        ConnectionState.DISCONNECTED_FOREVER,
        err.message
      )
    }
  }

  public isConnected(): boolean {
    console.log("You called isConnected.") // TODO: cleanup
    return this.connectionState === ConnectionState.CONNECTED
  }

  public isStaticConnection(): boolean {
    console.log("You called isStaticConnection.") // TODO: cleanup
    return this.connectionState === ConnectionState.STATIC
  }

  public incrementMessageCacheRunCount(maxCachedMessageAge: number): void {
    console.log(
      `You called incrementMessageCacheRunCount with ${maxCachedMessageAge}`
    ) // TODO: cleanup
    // StaticConnection does not use a MessageCache.
    if (this.connection instanceof WebsocketConnection) {
      this.connection.incrementMessageCacheRunCount(maxCachedMessageAge)
    }
  }

  public getBaseUriParts(): BaseUriParts | undefined {
    console.log("You called getBaseUriParts.") // TODO: cleanup
    if (this.connection instanceof WebsocketConnection) {
      return this.connection.getBaseUriParts()
    }
    return undefined
  }

  public sendMessage(msg: BackMsg): void {
    console.log(`You called sendMessage with ${msg}`) // TODO: cleanup
    if (this.connection instanceof WebsocketConnection && this.isConnected()) {
      this.connection.sendMessage(msg)
    } else {
      // Don't need to make a big deal out of this. Just print to console.
      logError(`Cannot send message when server is disconnected: ${msg}`)
    }
  }

  private showRetryError = (
    totalRetries: number,
    latestError: ReactNode,
    // The last argument of this function is unused and exists because the
    // WebsocketConnection.OnRetry type allows a third argument to be set to be
    // used in tests.
    _retryTimeout: number
  ): void => {
    // TODO: it seems like nothing ever increments this!
    if (totalRetries === RETRY_COUNT_FOR_WARNING) {
      this.props.onConnectionError(latestError)
    }
  }

  private connectToRunningServer(): WebsocketConnection {
    const baseUriPartsList = getPossibleBaseUris()

    return new WebsocketConnection({
      sessionInfo: this.props.sessionInfo,
      endpoints: this.props.endpoints,
      baseUriPartsList,
      onMessage: this.props.onMessage,
      onConnectionStateChange: this.setConnectionState,
      onRetry: this.showRetryError,
      claimHostAuthToken: this.props.claimHostAuthToken,
      resetHostAuthToken: this.props.resetHostAuthToken,
      onHostConfigResp: this.props.onHostConfigResp,
    })
  }

  /**
   * Opens either a static connection or a websocket connection, based on what
   * the manifest says.
   */
  private async connectBasedOnManifest(
    scriptRunId: string
  ): Promise<WebsocketConnection | StaticConnection> {
    const manifest = await ConnectionManager.fetchManifest(scriptRunId)

    return manifest.serverStatus === StaticManifest.ServerStatus.RUNNING
      ? this.connectToRunningServerFromManifest(manifest)
      : this.connectToStaticReportFromManifest(scriptRunId, manifest)
  }

  private connectToRunningServerFromManifest(
    manifest: any
  ): WebsocketConnection {
    const {
      configuredServerAddress,
      internalServerIP,
      externalServerIP,
      serverPort,
      serverBasePath,
    } = manifest

    const parts = { port: serverPort, basePath: serverBasePath }

    const baseUriPartsList = configuredServerAddress
      ? [{ ...parts, host: configuredServerAddress }]
      : [
          { ...parts, host: externalServerIP },
          { ...parts, host: internalServerIP },
        ]

    return new WebsocketConnection({
      sessionInfo: this.props.sessionInfo,
      endpoints: this.props.endpoints,
      baseUriPartsList,
      onMessage: this.props.onMessage,
      onConnectionStateChange: s => this.setConnectionState(s),
      onRetry: this.showRetryError,
      claimHostAuthToken: this.props.claimHostAuthToken,
      resetHostAuthToken: this.props.resetHostAuthToken,
      onHostConfigResp: this.props.onHostConfigResp,
    })
  }

  private connectToStaticReportFromManifest(
    scriptRunId: string,
    manifest: StaticManifest
  ): StaticConnection {
    return new StaticConnection({
      manifest,
      scriptRunId,
      onMessage: this.props.onMessage,
      onConnectionStateChange: s => this.setConnectionState(s),
    })
  }

  private static async fetchManifest(
    scriptRunId: string
  ): Promise<StaticManifest> {
    try {
      const data = await getReportObject(scriptRunId, "manifest.pb")
      const arrayBuffer = await data.arrayBuffer()

      return StaticManifest.decode(new Uint8Array(arrayBuffer))
    } catch (err) {
      logError(err)
      throw new Error("Unable to fetch data.")
    }
  }
}
