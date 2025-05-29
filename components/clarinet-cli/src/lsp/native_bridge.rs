use super::utils;
use crate::lsp::clarity_diagnostics_to_tower_lsp_type;
use clarity_lsp::backend::{
    process_mutating_request, process_notification, process_request, EditorStateInput,
    LspNotification, LspNotificationResponse, LspRequest, LspRequestResponse,
};
use clarity_lsp::lsp_types::{
    DocumentSymbolParams, DocumentSymbolResponse, GotoDefinitionParams, GotoDefinitionResponse,
    SignatureHelp, SignatureHelpParams,
};
use clarity_lsp::state::EditorState;
use crossbeam_channel::{Receiver as MultiplexableReceiver, Select, Sender as MultiplexableSender};
use serde_json::Value;
use std::fmt::Display;
use std::sync::mpsc::{Receiver, Sender};
use std::sync::{Arc, Mutex};
use tower_lsp::jsonrpc::{Error, ErrorCode, Result};
use tower_lsp::lsp_types::{
    CompletionParams, CompletionResponse, DidChangeTextDocumentParams, DidCloseTextDocumentParams,
    DidOpenTextDocumentParams, DidSaveTextDocumentParams, DocumentFormattingParams,
    DocumentRangeFormattingParams, ExecuteCommandParams, Hover, HoverParams, InitializeParams,
    InitializeResult, InitializedParams, MessageType, TextEdit, Url,
};
use tower_lsp::{async_trait, Client, LanguageServer};

pub enum LspResponse {
    Notification(LspNotificationResponse),
    Request(LspRequestResponse),
}

#[derive(Debug, Default)]
pub struct NativeBridgeConfig {
    debug_logging: bool,
}

impl NativeBridgeConfig {
    pub fn update(&mut self, editor_state: &EditorState) {
        self.debug_logging = editor_state.settings.debug_logging
    }
}

pub async fn start_language_server(
    notification_rx: MultiplexableReceiver<LspNotification>,
    request_rx: MultiplexableReceiver<LspRequest>,
    response_tx: Sender<LspResponse>,
    native_bridge_config: Arc<Mutex<NativeBridgeConfig>>,
) {
    let mut editor_state = EditorStateInput::Owned(EditorState::new());

    let mut sel = Select::new();
    let notifications_oper = sel.recv(&notification_rx);
    let requests_oper = sel.recv(&request_rx);

    loop {
        let oper = sel.select();
        match oper.index() {
            i if i == notifications_oper => match oper.recv(&notification_rx) {
                Ok(notification) => {
                    let result = process_notification(notification, &mut editor_state, None).await;
                    if let Ok(response) = result {
                        let _ = response_tx.send(LspResponse::Notification(response));
                    }
                }
                Err(_e) => {
                    continue;
                }
            },
            i if i == requests_oper => match oper.recv(&request_rx) {
                Ok(request) => {
                    let request_result = match request {
                        LspRequest::Initialize(_) => {
                            process_mutating_request(request, &mut editor_state).inspect(|_| {
                                // If `editor_state` may have changed, update native bridge config
                                match &editor_state {
                                    EditorStateInput::Owned(s) => {
                                        native_bridge_config.lock().unwrap().update(s)
                                    }
                                    EditorStateInput::RwLock(rwlock) => {
                                        // We explicitly initialize this as `EditorStateInput::Owned`, so this isn't really necessary
                                        let s = rwlock.read().unwrap();
                                        native_bridge_config.lock().unwrap().update(&s)
                                    }
                                };
                            })
                        }
                        _ => process_request(request, &editor_state),
                    };
                    if let Ok(response) = request_result {
                        let _ = response_tx.send(LspResponse::Request(response));
                    }
                }
                Err(_e) => {
                    continue;
                }
            },
            _ => unreachable!(),
        }
    }
}

#[derive(Debug)]
pub struct LspNativeBridge {
    client: Client,
    notification_tx: Arc<Mutex<MultiplexableSender<LspNotification>>>,
    request_tx: Arc<Mutex<MultiplexableSender<LspRequest>>>,
    response_rx: Arc<Mutex<Receiver<LspResponse>>>,
    config: Arc<Mutex<NativeBridgeConfig>>,
}

impl LspNativeBridge {
    pub fn new(
        client: Client,
        notification_tx: MultiplexableSender<LspNotification>,
        request_tx: MultiplexableSender<LspRequest>,
        response_rx: Receiver<LspResponse>,
        config: Arc<Mutex<NativeBridgeConfig>>,
    ) -> Self {
        Self {
            client,
            notification_tx: Arc::new(Mutex::new(notification_tx)),
            request_tx: Arc::new(Mutex::new(request_tx)),
            response_rx: Arc::new(Mutex::new(response_rx)),
            config,
        }
    }

    #[inline(always)]
    async fn debug<D: Display>(&self, message: D) {
        let logging_enabled = {
            // Put this in it's own scope to make sure `MutexGuard` is dropped before calling `log_message()`
            self.config.lock().unwrap().debug_logging
        };
        if logging_enabled {
            self.client.log_message(MessageType::LOG, message).await;
        }
    }
}

#[async_trait]
impl LanguageServer for LspNativeBridge {
    async fn initialize(&self, params: InitializeParams) -> Result<InitializeResult> {
        self.debug(format!("Recieved message `Initialize`: {params:?}"))
            .await;
        let _ = match self.request_tx.lock() {
            Ok(tx) => tx.send(LspRequest::Initialize(Box::new(params))),
            Err(_) => return Err(Error::new(ErrorCode::InternalError)),
        };

        let response_rx = self.response_rx.lock().expect("failed to lock response_rx");
        let response = &response_rx.recv().expect("failed to get value from recv");
        if let LspResponse::Request(LspRequestResponse::Initialize(initialize)) = response {
            return Ok(*initialize.to_owned());
        }
        Err(Error::new(ErrorCode::InternalError))
    }

    async fn initialized(&self, params: InitializedParams) {
        self.debug(format!("Recieved message `Initialized`: {params:?}"))
            .await;
    }

    async fn shutdown(&self) -> Result<()> {
        self.debug("Recieved message `Shutdown`").await;
        Ok(())
    }

    async fn execute_command(&self, params: ExecuteCommandParams) -> Result<Option<Value>> {
        self.debug(format!("Recieved message `ExecuteCommand`: {params:?}"))
            .await;
        Ok(None)
    }

    async fn completion(&self, params: CompletionParams) -> Result<Option<CompletionResponse>> {
        let _ = match self.request_tx.lock() {
            Ok(tx) => tx.send(LspRequest::Completion(params)),
            Err(_) => return Ok(None),
        };

        let response_rx = self.response_rx.lock().expect("failed to lock response_rx");
        let response = &response_rx.recv().expect("failed to get value from recv");
        if let LspResponse::Request(LspRequestResponse::CompletionItems(items)) = response {
            return Ok(Some(CompletionResponse::from(items.to_vec())));
        }

        Ok(None)
    }

    async fn goto_definition(
        &self,
        params: GotoDefinitionParams,
    ) -> Result<Option<GotoDefinitionResponse>> {
        self.debug(format!("Recieved message `GotoDefinition`: {params:?}"))
            .await;
        let _ = match self.request_tx.lock() {
            Ok(tx) => tx.send(LspRequest::Definition(params)),
            Err(_) => return Ok(None),
        };

        let response_rx = self.response_rx.lock().expect("failed to lock response_rx");
        let response = &response_rx.recv().expect("failed to get value from recv");
        if let LspResponse::Request(LspRequestResponse::Definition(Some(data))) = response {
            return Ok(Some(GotoDefinitionResponse::Scalar(data.to_owned())));
        }

        Ok(None)
    }

    async fn document_symbol(
        &self,
        params: DocumentSymbolParams,
    ) -> Result<Option<DocumentSymbolResponse>> {
        self.debug(format!("Recieved message `DocumentSymbol`: {params:?}"))
            .await;
        let _ = match self.request_tx.lock() {
            Ok(tx) => tx.send(LspRequest::DocumentSymbol(params)),
            Err(_) => return Ok(None),
        };

        let response_rx = self.response_rx.lock().expect("failed to lock response_rx");
        let response = &response_rx.recv().expect("failed to get value from recv");
        if let LspResponse::Request(LspRequestResponse::DocumentSymbol(symbols)) = response {
            return Ok(Some(DocumentSymbolResponse::Nested(symbols.to_vec())));
        }

        Ok(None)
    }

    async fn hover(&self, params: HoverParams) -> Result<Option<Hover>> {
        self.debug(format!("Recieved message `Hover`: {params:?}"))
            .await;
        let _ = match self.request_tx.lock() {
            Ok(tx) => tx.send(LspRequest::Hover(params)),
            Err(_) => return Ok(None),
        };

        let response_rx = self.response_rx.lock().expect("failed to lock response_rx");
        let response = &response_rx.recv().expect("failed to get value from recv");
        if let LspResponse::Request(LspRequestResponse::Hover(data)) = response {
            return Ok(data.to_owned());
        }

        Ok(None)
    }

    async fn signature_help(&self, params: SignatureHelpParams) -> Result<Option<SignatureHelp>> {
        self.debug(format!("Recieved message `SignatureHelp`: {params:?}"))
            .await;
        let _ = match self.request_tx.lock() {
            Ok(tx) => tx.send(LspRequest::SignatureHelp(params)),
            Err(_) => return Ok(None),
        };

        let response_rx = self.response_rx.lock().expect("failed to lock response_rx");
        let response = &response_rx.recv().expect("failed to get value from recv");
        if let LspResponse::Request(LspRequestResponse::SignatureHelp(data)) = response {
            return Ok(data.to_owned());
        }

        Ok(None)
    }

    async fn formatting(&self, params: DocumentFormattingParams) -> Result<Option<Vec<TextEdit>>> {
        self.debug(format!("Recieved message `Formatting`: {params:?}"))
            .await;
        let _ = match self.request_tx.lock() {
            Ok(tx) => tx.send(LspRequest::DocumentFormatting(params)),
            Err(_) => return Ok(None),
        };

        let response_rx = self.response_rx.lock().expect("failed to lock response_rx");
        let response = &response_rx.recv().expect("failed to get value from recv");
        if let LspResponse::Request(LspRequestResponse::DocumentFormatting(data)) = response {
            return Ok(data.to_owned());
        }

        Ok(None)
    }

    async fn range_formatting(
        &self,
        params: DocumentRangeFormattingParams,
    ) -> Result<Option<Vec<TextEdit>>> {
        self.debug(format!("Recieved message `RangeFormatting`: {params:?}"))
            .await;
        let _ = match self.request_tx.lock() {
            Ok(tx) => tx.send(LspRequest::DocumentRangeFormatting(params)),
            Err(_) => return Ok(None),
        };

        let response_rx = self.response_rx.lock().expect("failed to lock response_rx");
        let response = &response_rx.recv().expect("failed to get value from recv");
        if let LspResponse::Request(LspRequestResponse::DocumentRangeFormatting(data)) = response {
            return Ok(data.to_owned());
        }

        Ok(None)
    }

    async fn did_open(&self, params: DidOpenTextDocumentParams) {
        self.debug(format!("Recieved message `DidOpen`: {params:?}"))
            .await;
        if let Some(contract_location) = utils::get_contract_location(&params.text_document.uri) {
            let _ = match self.notification_tx.lock() {
                Ok(tx) => tx.send(LspNotification::ContractOpened(contract_location)),
                Err(_) => return,
            };
        } else if let Some(manifest_location) =
            utils::get_manifest_location(&params.text_document.uri)
        {
            let _ = match self.notification_tx.lock() {
                Ok(tx) => tx.send(LspNotification::ManifestOpened(manifest_location)),
                Err(_) => return,
            };
        } else {
            self.client
                .log_message(MessageType::WARNING, "Unsupported file opened")
                .await;
            return;
        };

        self.client
            .log_message(
                MessageType::WARNING,
                "Command submitted to background thread",
            )
            .await;
        let mut aggregated_diagnostics = vec![];
        let mut notification = None;
        if let Ok(response_rx) = self.response_rx.lock() {
            if let Ok(LspResponse::Notification(ref mut notification_response)) = response_rx.recv()
            {
                aggregated_diagnostics.append(&mut notification_response.aggregated_diagnostics);
                notification = notification_response.notification.take();
            }
        }
        for (location, mut diags) in aggregated_diagnostics.into_iter() {
            if let Ok(url) = location.to_url_string() {
                self.client
                    .publish_diagnostics(
                        Url::parse(&url).unwrap(),
                        clarity_diagnostics_to_tower_lsp_type(&mut diags),
                        None,
                    )
                    .await;
            }
        }
        if let Some((level, message)) = notification {
            self.client.show_message(level, message).await;
        }
    }

    async fn did_save(&self, params: DidSaveTextDocumentParams) {
        self.debug(format!("Recieved message `DidSave`: {params:?}"))
            .await;
        if let Some(contract_location) = utils::get_contract_location(&params.text_document.uri) {
            let _ = match self.notification_tx.lock() {
                Ok(tx) => tx.send(LspNotification::ContractSaved(contract_location)),
                Err(_) => return,
            };
        } else if let Some(manifest_location) =
            utils::get_manifest_location(&params.text_document.uri)
        {
            let _ = match self.notification_tx.lock() {
                Ok(tx) => tx.send(LspNotification::ManifestSaved(manifest_location)),
                Err(_) => return,
            };
        } else {
            return;
        };

        let mut aggregated_diagnostics = vec![];
        let mut notification = None;
        if let Ok(response_rx) = self.response_rx.lock() {
            if let Ok(LspResponse::Notification(ref mut notification_response)) = response_rx.recv()
            {
                aggregated_diagnostics.append(&mut notification_response.aggregated_diagnostics);
                notification = notification_response.notification.take();
            }
        }

        for (location, mut diags) in aggregated_diagnostics.into_iter() {
            if let Ok(url) = location.to_url_string() {
                self.client
                    .publish_diagnostics(
                        Url::parse(&url).unwrap(),
                        clarity_diagnostics_to_tower_lsp_type(&mut diags),
                        None,
                    )
                    .await;
            }
        }
        if let Some((level, message)) = notification {
            self.client.show_message(level, message).await;
        }
    }

    async fn did_change(&self, params: DidChangeTextDocumentParams) {
        self.debug(format!("Recieved message `DidChange`: {params:?}"))
            .await;
        if let Some(contract_location) = utils::get_contract_location(&params.text_document.uri) {
            if let Ok(tx) = self.notification_tx.lock() {
                let _ = tx.send(LspNotification::ContractChanged(
                    contract_location,
                    params.content_changes[0].text.to_string(),
                ));
            };
        }
    }

    async fn did_close(&self, params: DidCloseTextDocumentParams) {
        self.debug(format!("Recieved message `DidClose`: {params:?}"))
            .await;
        if let Some(contract_location) = utils::get_contract_location(&params.text_document.uri) {
            if let Ok(tx) = self.notification_tx.lock() {
                let _ = tx.send(LspNotification::ContractClosed(contract_location));
            };
        }
    }
}
