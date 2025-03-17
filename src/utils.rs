use std::ptr::NonNull;

pub(crate) struct SendSafeNonNull<T: ?Sized>(NonNull<T>);

impl<T: ?Sized> SendSafeNonNull<T> {
    pub(crate) unsafe fn new(ptr: NonNull<T>) -> Self {
        Self(ptr)
    }

    pub(crate) unsafe fn as_mut<'a>(&mut self) -> &'a mut T {
        unsafe { self.0.as_mut() }
    }
}

unsafe impl<T: Send + ?Sized> Send for SendSafeNonNull<T> {}
