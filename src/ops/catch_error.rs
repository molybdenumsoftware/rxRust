use std::marker::PhantomData;

use crate::{
  CoreObservable, IntoBoxedSubscription, Observable, Subscription,
  context::{Context, RcDerefMut},
  observable::ObservableType,
  observer::Observer,
};

/// The CatchError operator struct.
///
/// # Example
///
/// ```
/// use rxrust::prelude::*;
///
///  let observable = Local::throw_err("some-error")
///    .catch_error(|error| Local::from_iter([format!("error: {error}"), String::from("after")]));
///  let mut result = Vec::new();
///  observable.subscribe(|v| {
///    result.push(v);
///  });
///  assert_eq!(result, vec![String::from("error: some-error"), String::from("after")]);
/// ```
pub struct CatchError<S, F> {
  pub source: S,
  pub func: F,
}

pub struct CatchErrorOrigObserver<P, O, F, SO> {
  subscription: P,
  observer: O,
  func: F,
  subst_observable: PhantomData<SO>,
}

impl<NextObserver, Item, OrigErr, F, SubstObservable> Observer<Item, OrigErr>
  for CatchErrorOrigObserver<
    NextObserver::RcMut<Option<NextObserver::BoxedSubscription>>,
    NextObserver,
    F,
    SubstObservable,
  >
where
  NextObserver: Context + Observer<Item, SubstObservable::Err>,
  F: FnOnce(OrigErr) -> NextObserver::With<SubstObservable>,
  SubstObservable: CoreObservable<NextObserver>,
  // NextObserver: Observer<_, _>,
  // P: RcDerefMut<Target = Option<SubstObservable::BoxedSubscription>>,
  // NextObserver: Observer<Item, SubstObservable::Err>,
  // SubstUnsub: IntoBoxedSubscription<SubstObservable::Inner::BoxedSubscription>,
  // Context::With ?
  // SubstObservable:
  // CoreObservable<Ctx::With<CoreObservable<CatchErrorSubstObserver<NextObserver>, Unsub = SubstUnsub>>> + 'static,
  // Observable,
  //Context + CoreObservable<CatchErrorSubstObserver<NextObserver>, Unsub = SubstUnsub> + 'static,
  //SubstObservable: Context<Inner: ObservableType<Err = SubstErr> + 'static>,
{
  fn next(&mut self, value: Item) {
    self.observer.next(value);
  }

  fn error(self, err: OrigErr) {
    let Self { subscription, observer, func, .. } = self;
    if let Some(sub) = subscription.rc_deref_mut().take() {
      sub.unsubscribe();
    }
    let subst_observable = func(err);
    // let subst_observer = CatchErrorSubstObserver { observer };
    *subscription.rc_deref_mut() = Some(
      subst_observable
        .subscribe_with(observer)
        .into_boxed(),
    );
  }

  fn complete(self) {
    self.observer.complete();
  }

  fn is_closed(&self) -> bool {
    self.observer.is_closed()
  }
}

pub struct CatchErrorSubstObserver<O> {
  observer: O,
}

impl<O, Item, Err> Observer<Item, Err> for CatchErrorSubstObserver<O>
where
  O: Observer<Item, Err>,
{
  fn next(&mut self, value: Item) {
    self.observer.next(value)
  }

  fn error(self, err: Err) {
    self.observer.error(err)
  }

  fn complete(self) {
    self.observer.complete();
  }

  fn is_closed(&self) -> bool {
    self.observer.is_closed()
  }
}

impl<S, F, SubstObservable, SubstErr> ObservableType for CatchError<S, F>
where
  S: ObservableType,
  F: FnOnce(S::Err) -> SubstObservable,
  SubstObservable: Context<Inner: ObservableType<Err = SubstErr> + 'static>,
{
  type Item<'a>
    = S::Item<'a>
  where
    Self: 'a;
  type Err = SubstErr;
}

impl<S, F, SubstObservable, SubstErr, Ctx> CoreObservable<Ctx> for CatchError<S, F>
where
  Ctx: Context + for<'a> Observer<SubstObservable::Item<'a>, SubstErr>,
  S: CoreObservable<
    Ctx::With<
      CatchErrorOrigObserver<
        Ctx::RcMut<Option<Ctx::BoxedSubscription>>,
        Ctx::Inner,
        F,
        SubstObservable,
      >,
    >,
  >,
  S::Unsub: IntoBoxedSubscription<Ctx::BoxedSubscription>,
  F: FnOnce(S::Err) -> SubstObservable,
  SubstObservable: Context<Inner: ObservableType<Err = SubstErr> + 'static> + ObservableType,
  Ctx::RcMut<Option<Ctx::BoxedSubscription>>: Subscription,
{
  type Unsub = Ctx::RcMut<Option<Ctx::BoxedSubscription>>;

  fn subscribe(self, context: Ctx) -> Self::Unsub {
    let Self { source, func } = self;
    let subscription = Ctx::RcMut::from(None);
    let subscription_clone = subscription.clone();
    let wrapped = context.transform(move |observer| CatchErrorOrigObserver {
      subscription: subscription_clone,
      observer,
      func,
      subst_observable: PhantomData,
    });
    *subscription.rc_deref_mut() = Some(source.subscribe(wrapped).into_boxed());
    subscription
  }
}
