use std::{
    fmt::{Display, Formatter},
    sync::{Arc, RwLock},
};

use async_trait::async_trait;
use cqrs_es::{Aggregate, DomainEvent, EventEnvelope, Query, event_sink::EventSink};
use eventsourcingdb_es::{conversion::ReversedDomain, cqrs::esdb_cqrs, types::EventSourcingDbCqrs};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub enum BankAccountCommand {
    OpenAccount { account_id: String },
    DepositMoney { amount: f64 },
    WithdrawMoney { amount: f64, atm_id: String },
    WriteCheck { check_number: String, amount: f64 },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum BankAccountEvent {
    AccountOpened {
        account_id: String,
    },
    CustomerDepositedMoney {
        amount: f64,
        balance: f64,
    },
    CustomerWithdrewCash {
        amount: f64,
        balance: f64,
    },
    CustomerWroteCheck {
        check_number: String,
        amount: f64,
        balance: f64,
    },
}

impl DomainEvent for BankAccountEvent {
    fn event_type(&self) -> String {
        let event_type: &str = match self {
            BankAccountEvent::AccountOpened { .. } => "AccountOpened",
            BankAccountEvent::CustomerDepositedMoney { .. } => "CustomerDepositedMoney",
            BankAccountEvent::CustomerWithdrewCash { .. } => "CustomerWithdrewCash",
            BankAccountEvent::CustomerWroteCheck { .. } => "CustomerWroteCheck",
        };
        event_type.to_string()
    }

    fn event_version(&self) -> String {
        "1.0".to_string()
    }
}

#[derive(Debug)]
pub struct BankAccountError(String);

impl Display for BankAccountError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::error::Error for BankAccountError {}

impl From<&str> for BankAccountError {
    fn from(message: &str) -> Self {
        BankAccountError(message.to_string())
    }
}

pub struct BankAccountServices;

impl BankAccountServices {
    async fn atm_withdrawal(&self, _atm_id: &str, _amount: f64) -> Result<(), AtmError> {
        Ok(())
    }

    async fn validate_check(&self, _account: &str, _check: &str) -> Result<(), CheckingError> {
        Ok(())
    }
}
pub struct AtmError;
pub struct CheckingError;

#[derive(Serialize, Default, Deserialize)]
pub struct BankAccount {
    account_id: String,
    // this is a floating point for our example, don't do this IRL
    balance: f64,
}

impl Aggregate for BankAccount {
    // This identifier should be unique to the system.
    const TYPE: &'static str = "account";
    type Command = BankAccountCommand;
    type Event = BankAccountEvent;
    type Error = BankAccountError;
    type Services = BankAccountServices;

    // The aggregate logic goes here. Note that this will be the _bulk_ of a CQRS system
    // so expect to use helper functions elsewhere to keep the code clean.
    async fn handle(
        &mut self,
        command: Self::Command,
        services: &Self::Services,
        sink: &EventSink<Self>,
    ) -> Result<(), Self::Error> {
        match command {
            BankAccountCommand::OpenAccount { account_id } => {
                sink.write(BankAccountEvent::AccountOpened { account_id }, self)
                    .await;
            }
            BankAccountCommand::DepositMoney { amount } => {
                let balance = self.balance + amount;
                sink.write(
                    BankAccountEvent::CustomerDepositedMoney { amount, balance },
                    self,
                )
                .await;
            }
            BankAccountCommand::WithdrawMoney { amount, atm_id } => {
                let balance = self.balance - amount;
                if balance < 0_f64 {
                    return Err("funds not available".into());
                }
                if services.atm_withdrawal(&atm_id, amount).await.is_err() {
                    return Err("atm rule violation".into());
                }
                sink.write(
                    BankAccountEvent::CustomerWithdrewCash { amount, balance },
                    self,
                )
                .await;
            }
            BankAccountCommand::WriteCheck {
                check_number,
                amount,
            } => {
                let balance = self.balance - amount;
                if balance < 0_f64 {
                    return Err("funds not available".into());
                }
                if services
                    .validate_check(&self.account_id, &check_number)
                    .await
                    .is_err()
                {
                    return Err("check invalid".into());
                }
                sink.write(
                    BankAccountEvent::CustomerWroteCheck {
                        check_number,
                        amount,
                        balance,
                    },
                    self,
                )
                .await;
            }
        };
        Ok(())
    }

    fn apply(&mut self, event: Self::Event) {
        match event {
            BankAccountEvent::AccountOpened { account_id } => {
                self.account_id = account_id;
            }
            BankAccountEvent::CustomerDepositedMoney { amount: _, balance }
            | BankAccountEvent::CustomerWithdrewCash { amount: _, balance }
            | BankAccountEvent::CustomerWroteCheck {
                check_number: _,
                amount: _,
                balance,
            } => {
                self.balance = balance;
            }
        }
    }
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct BankAccountView {
    account_id: String,
    balance: f64,
    activity: Vec<String>,
}

#[derive(Clone)]
pub struct BankAccountQuery {
    view: Arc<RwLock<BankAccountView>>,
}

impl BankAccountQuery {
    fn new(view: Arc<RwLock<BankAccountView>>) -> Self {
        Self { view }
    }
}

#[async_trait]
impl Query<BankAccount> for BankAccountQuery {
    async fn dispatch(&self, aggregate_id: &str, events: &[EventEnvelope<BankAccount>]) {
        let mut view = self.view.write().expect("bank account view lock poisoned");

        for event in events {
            view.account_id = aggregate_id.to_string();

            match &event.payload {
                BankAccountEvent::AccountOpened { account_id } => {
                    view.account_id = account_id.clone();
                    view.activity
                        .push(format!("opened account {account_id}"));
                }
                BankAccountEvent::CustomerDepositedMoney { amount, balance } => {
                    view.balance = *balance;
                    view.activity
                        .push(format!("deposited {amount:.2}, balance is now {balance:.2}"));
                }
                BankAccountEvent::CustomerWithdrewCash { amount, balance } => {
                    view.balance = *balance;
                    view.activity
                        .push(format!("withdrew {amount:.2}, balance is now {balance:.2}"));
                }
                BankAccountEvent::CustomerWroteCheck {
                    check_number,
                    amount,
                    balance,
                } => {
                    view.balance = *balance;
                    view.activity.push(format!(
                        "wrote check {check_number} for {amount:.2}, balance is now {balance:.2}"
                    ));
                }
            }
        }
    }
}

async fn execute_command(
    cqrs: &EventSourcingDbCqrs<BankAccount>,
    aggregate_id: &str,
    label: &str,
    command: BankAccountCommand,
) {
    cqrs.execute(aggregate_id, command)
        .await
        .unwrap_or_else(|err| panic!("{label} failed: {err}"));
    println!("executed: {label}");
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let client = eventsourcingdb::Client::new(
        url::Url::parse("http://localhost:3000").unwrap(),
        "secret".to_string(),
    );

    let domain = ReversedDomain::new(vec!["com", "flangator", "banking"]);
    let aggregate_id = "account-0001";
    let view = Arc::new(RwLock::new(BankAccountView::default()));
    let query = BankAccountQuery::new(Arc::clone(&view));
    let cqrs = esdb_cqrs::<BankAccount>(client, domain, vec![Box::new(query)], BankAccountServices);

    execute_command(
        &cqrs,
        aggregate_id,
        "open account",
        BankAccountCommand::OpenAccount {
            account_id: aggregate_id.to_string(),
        },
    )
    .await;
    execute_command(
        &cqrs,
        aggregate_id,
        "deposit 250.00",
        BankAccountCommand::DepositMoney { amount: 250.0 },
    )
    .await;
    execute_command(
        &cqrs,
        aggregate_id,
        "withdraw 40.00 from ATM-7",
        BankAccountCommand::WithdrawMoney {
            amount: 40.0,
            atm_id: "ATM-7".to_string(),
        },
    )
    .await;
    execute_command(
        &cqrs,
        aggregate_id,
        "write check CHK-1001 for 25.00",
        BankAccountCommand::WriteCheck {
            check_number: "CHK-1001".to_string(),
            amount: 25.0,
        },
    )
    .await;
    execute_command(
        &cqrs,
        aggregate_id,
        "deposit 10.00",
        BankAccountCommand::DepositMoney { amount: 10.0 },
    )
    .await;

    let view = view.read().expect("bank account view lock poisoned");

    println!();
    println!("projection for {}:", view.account_id);
    println!("final balance: {:.2}", view.balance);
    println!("expected final balance: 195.00");
    println!("activity:");
    for entry in &view.activity {
        println!(" - {entry}");
    }
}
