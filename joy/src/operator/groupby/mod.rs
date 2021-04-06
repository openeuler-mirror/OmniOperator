use inkwell::values::IntValue;

pub mod hash_groupby;
pub mod perfect_id_groupby;

//FIXME: Aggregator should only be used in GroupBy, find a way to control access
pub trait Aggregator {
    fn init(&self);
    fn process(&self);
}

pub struct SumAggregator<'ctx> {
    input: i32,
    state: Option<IntValue<'ctx>>,
}

impl<'ctx> Aggregator for SumAggregator<'ctx> {
    fn init(&self) {
        unimplemented!()
    }

    fn process(&self) {
        unimplemented!()
    }
}

pub struct CountAggregator {
    input: i32,
}

impl Aggregator for CountAggregator {
    fn init(&self) {
        unimplemented!()
    }

    fn process(&self) {
        unimplemented!()
    }
}
