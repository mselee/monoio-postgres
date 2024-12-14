use monoio::io::stream::Stream;
use postgres_types::ToSql;

pub trait TryStreamExt: Stream {
    /// Collects all items from the stream into a container, such as a Vec.
    async fn try_collect<C, T, E>(mut self) -> Result<C, E>
    where
        Self: Sized + Stream<Item = Result<T, E>>,
        C: Default + Extend<T>,
    {
        let mut items = C::default();
        while let Some(item) = self.next().await {
            match item {
                Ok(item) => items.extend(Some(item)),
                Err(err) => return Err(err),
            }
        }
        Ok(items)
    }

    async fn try_next<T, E>(&mut self) -> Result<Option<T>, E>
    where
        Self: Stream<Item = Result<T, E>>,
    {
        self.next().await.transpose()
    }

    async fn try_drain<T, E>(&mut self) -> Result<(), E>
    where
        Self: Stream<Item = Result<T, E>>,
    {
        while let Some(item) = self.next().await {
            if let Err(err) = item {
                return Err(err);
            }
        }
        Ok(())
    }
}

impl<T, E, S: ?Sized + Stream<Item = Result<T, E>>> TryStreamExt for S {}

pub(crate) fn slice_iter<'a>(
    s: &'a [&'a (dyn ToSql)],
) -> impl ExactSizeIterator<Item = &'a dyn ToSql> + 'a {
    s.iter().map(|s| *s as _)
}
