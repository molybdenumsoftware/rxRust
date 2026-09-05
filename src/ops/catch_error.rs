use std::marker::PhantomData;

use crate::{
  BoxedSubscription, CoreObservable, IntoBoxedSubscription, Observable, Subscription,
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

pub struct CatchErrorOrigObserver<P, O, F, SO, C, NO> {
  subscription: P,
  observer: O,
  func: F,
  subst_observable: PhantomData<SO>,
  ctx: PhantomData<C>,
  next_observer: PhantomData<NO>,
}

impl<Ctx, NextObserver, Item, OrigErr, F, SubstObservable> Observer<Item, OrigErr>
  for CatchErrorOrigObserver<
    Ctx::RcMut<Option<BoxedSubscription>>,
    Ctx::With<NextObserver>,
    F,
    SubstObservable,
    Ctx,
    NextObserver,
  >
where
  Ctx: Context,
  NextObserver: Observer<Item, SubstObservable::Err>,
  F: FnOnce(OrigErr) -> Ctx::With<SubstObservable>,
  SubstObservable: CoreObservable<Ctx::With<NextObserver>, Unsub: 'static>,
  // `CoreObservable<NextObserver::With<SubstObservable>::With<_>>` is not implemented for `SubstObservable`
  // SubstObservable: NextObserver::With<CoreObservable<NextObserver::With<NextObserver>>>,
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
    self.observer.inner_mut().next(value);
  }

  fn error(self, err: OrigErr) {
    let Self { subscription, observer, func, .. } = self;
    if let Some(sub) = subscription.rc_deref_mut().take() {
      sub.unsubscribe();
    }
    let mut subst_observable = func(err);
    // let subst_subscription = subst_observable.transform(|inner| {
    //   let subst_subscription = inner.subscribe(observer);
    //   subst_subscription
    // });
    // let subst_observer = CatchErrorSubstObserver { observer };
    let subst_subscription = subst_observable.into_inner().subscribe(observer);
    // let boxed = subst_subscription.in
    // let subst_subscription = subst_observable.subscribe_with(observer);
    // let boxed = Ctx::BoxedSubscription::into_boxed(subst_subscription);
    let boxed = subst_subscription.into_boxed();
    *subscription.rc_deref_mut() = Some(boxed);
  }

  fn complete(self) {
    self.observer.into_inner().complete();
  }

  fn is_closed(&self) -> bool {
    self.observer.inner().is_closed()
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
        Ctx,
        Ctx,
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
      ctx: PhantomData,
      next_observer: PhantomData,
    });
    *subscription.rc_deref_mut() = Some(source.subscribe(wrapped).into_boxed());
    subscription
  }
}
