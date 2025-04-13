pub struct Defer<F: FnOnce()> {
    action: Option<F>
}

impl<F: FnOnce()> Defer<F> {
    pub fn new(action: F) -> Self {
        Self {
            action: Some(action)
        }
    }
}

impl<F: FnOnce()> Drop for Defer<F> {
    fn drop(&mut self) {
        if let Some(action) = self.action.take() {
            action();
        }
    }
}

#[macro_export]
macro_rules! defer {
    ($($body:tt)*) => {
        let __defer_guard = $crate::kstd::defer::Defer::new(|| { $($body)* });
    };
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;
    use std::rc::Rc;

    #[test]
    fn test_defer() {
        let v = Rc::new(RefCell::new(Vec::new()));

        // 捕获外部变量的克隆
        let v_clone = v.clone();
        defer! {
            v_clone.borrow_mut().push(1);
        }

        v.borrow_mut().push(4);

        {
            let v_clone = v.clone();
            defer! {
                v_clone.borrow_mut().push(2);
            }
            v.borrow_mut().push(3);
        }

        assert_eq!(*v.borrow(), vec![4, 3, 2]);
        assert_eq!(v.borrow().len(), 3);
    }
}