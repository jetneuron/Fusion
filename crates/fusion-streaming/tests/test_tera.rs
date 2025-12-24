pub mod test {
    use tera::{Context, Tera};
    use fusion_streaming::utils::tera_func;

    static BIG_TABLE_TEMPLATE: &str = r#"
    [
        {% for row in table %}
        [{% for col in row %}{"id": {{ col }}}{% if not loop.last %},{% endif %}{% endfor %}]{% if not loop.last %},{% endif %}
        {% endfor %}
    ]
    {{str}}
    NowTs: {{now()}}
    Date: {{yyyyMMdd()}}
    Date: {{yyyy_MM_dd()}}
    HumanTime: {{humanTime()}}"#;

    #[tokio::test]
    async fn simple_tera_test() -> Result<(), Box<dyn std::error::Error>> {
        let size = 2;

        let mut table = Vec::with_capacity(size);
        for _ in 0..size {
            let mut inner = Vec::with_capacity(size);
            for i in 0..size {
                inner.push(i);
            }
            table.push(inner);
        }

        let mut tera = Tera::default();
        tera.register_function("yyyyMMdd", tera_func::yyyymmdd);
        tera.register_function("yyyy_MM_dd", tera_func::yyyy_mm_dd);
        tera.register_function("humanTime", tera_func::human_time);
        tera.register_function("now", tera_func::now);
        tera.add_raw_templates(vec![("big-table.html", BIG_TABLE_TEMPLATE)]).unwrap();
        let mut ctx = Context::new();
        ctx.insert("table", &table);
        ctx.insert("str", "=========== String Content ===========");

        let mut context = Context::new();
        context.insert("greeting", &"Hello");
        let string = tera.render_str("{{ greeting }} World! {{ yyyyMMdd() }}", &context)?;
        println!("{}", string);

        let content = tera.render("big-table.html", &ctx).unwrap();

        println!("{}", content);
        Ok(())
    }
}