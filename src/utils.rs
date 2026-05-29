use std::{cell::{Cell, RefCell}, rc::Rc};
use wasm_bindgen_futures::JsFuture;
use web_sys::{js_sys, window};

#[macro_export]
macro_rules! log_fmt {
    ($($arg:tt)*) => {
        {
            let msg = format!($($arg)*);
            web_sys::console::log_1(&JsValue::from_str(&msg));
            msg
        }
    };
}

#[macro_export]
macro_rules! clone_move {
    ($($n:ident),+ => $closure:expr) => {
        {
            $( let $n = $n.clone(); )+
            $closure
        }
    };
}

pub fn munch_u64(str: &str) -> Result<(u64, &str), ()>{
    let value_range = str.find(' ').unwrap_or(str.len());
    let Ok(value) = u64::from_str_radix(&str[..value_range], 10) else {
        return Err(())
    };
    Ok((value, str[value_range..].trim()))
}

pub async fn web_sleep(ms: i32) {
    let promise = js_sys::Promise::new(&mut |resolve, _| {
        window()
            .unwrap()
            .set_timeout_with_callback_and_timeout_and_arguments_0(&resolve, ms)
            .expect("should register setTimeout");
    });

    let _ = JsFuture::from(promise).await;
}

#[derive(Debug, Clone)]
pub struct NullSmartPtr<T>(SmartPtr<Option<T>>);
impl<T> NullSmartPtr<T> {
    pub fn null() -> Self {
        Self(SmartPtr::new(None))
    }
    pub fn new(t: T) -> Self {
        Self(SmartPtr::new(Some(t)))
    }
    pub fn is_some(&self)->bool{
        self.0.borrow().is_some()
    }
    pub fn is_none(&self)->bool{
        self.0.borrow().is_none()
    }
    pub fn set(&self, t: T){
        *self.0.borrow_mut() = Some(t);
    }
    pub fn borrow(&self) -> Option<std::cell::Ref<'_, T>> {
        let Ok(reference) = std::cell::Ref::filter_map(self.0.borrow(), |opt| {
            opt.as_ref()
        }) else {
            return None;
        };
        Some(reference)
    }
    pub fn borrow_mut(&self) -> Option<std::cell::RefMut<'_, T>> {
        let Ok(reference) = std::cell::RefMut::filter_map(self.0.borrow_mut(), |opt| {
            opt.as_mut()
        }) else {
            return None;
        };
        Some(reference)
    }
    pub fn try_unwrap(self) -> Result<T, ()>{
        match self.0.borrow_mut().take(){
            Some(value) => Ok(value),
            None => Err(()),
        }
    }
}

impl<T: Clone> NullSmartPtr<T> {
    pub fn try_clone_inner(&self)->Option<T>{
        self.borrow().map(|t| t.clone())
    }
}

#[derive(Debug, Clone)]
pub struct SmartCell<T: Copy>(Rc<Cell<T>>);
impl<T: Copy> SmartCell<T> {
    pub fn new(t: T) -> Self {
        Self(Rc::new(Cell::new(t)))
    }
}
impl<T:Copy> std::ops::Deref for SmartCell<T>{
    type Target = Cell<T>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[derive(Debug)]
pub struct SmartPtr<T>(Rc<RefCell<T>>);
impl<T> SmartPtr<T> {
    pub fn new(t: T) -> Self {
        Self(Rc::new(RefCell::new(t)))
    }
    pub fn set(&self, t: T){
        *self.0.borrow_mut() = t;
    }
    pub fn borrow(&self) -> std::cell::Ref<'_, T> {
        self.0.borrow()
    }
    pub fn borrow_mut(&self) -> std::cell::RefMut<'_, T> {
        self.0.borrow_mut()
    }
}
impl<T> Clone for SmartPtr<T>{
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

pub async fn wait_until<T, E>(tick_delay: u32, timeout: u32, timeout_error: E, mut condition: impl FnMut(u32)->Result<Option<T>, E>) -> Result<T, E>{
    let mut time = 0;
    loop {
        web_sleep(tick_delay as i32).await;
        let result = condition(time);
        if let Err(error) = result{
            return Err(error)
        }
        if let Ok(Some(result)) = result{
            return Ok(result)
        }
        time += u32::max(tick_delay, 1);
        if time >= timeout{
            return Err(timeout_error)
        }
    }
}