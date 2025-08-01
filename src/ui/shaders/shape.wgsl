struct VertexInput {
    @location(0) position: vec2<f32>,
    @location(1) color: vec4<f32>,
};

struct VertexOutput {
    @builtin(position) position: vec4<f32>,
    @location(0) color: vec4<f32>,
    @location(1) uv: vec2<f32>,
};

struct Uniforms {
    view_projection: mat4x4<f32>,
    shape_type: u32, // 0 = rectangle, 1 = circle, 2 = polygon
    filled: u32,
};

@group(0) @binding(0) var<uniform> uniforms: Uniforms;

@vertex
fn vs_main(input: VertexInput) -> VertexOutput {
    let world_pos = vec4<f32>(input.position, 0.0, 1.0);
    let clip_pos = uniforms.view_projection * world_pos;
    
    // Generate UV coordinates for shape
    let uv = input.position;
    
    return VertexOutput(
        clip_pos,
        input.color,
        uv,
    );
}

@fragment
fn fs_main(input: VertexOutput) -> @location(0) vec4<f32> {
    let uv = input.uv;
    let shape_type = uniforms.shape_type;
    let filled = uniforms.filled;
    
    var alpha: f32 = 1.0;
    
    if (shape_type == 0u) {
        // Rectangle
        let rect = step(vec2<f32>(-1.0, -1.0), uv) * step(uv, vec2<f32>(1.0, 1.0));
        alpha = rect.x * rect.y;
        
        if (filled == 0u) {
            // Outline only
            let border = 0.1;
            let inner_rect = step(vec2<f32>(-1.0 + border, -1.0 + border), uv) * 
                           step(uv, vec2<f32>(1.0 - border, 1.0 - border));
            alpha = alpha - (inner_rect.x * inner_rect.y);
        }
    } else if (shape_type == 1u) {
        // Circle
        let distance = length(uv);
        let radius = 1.0;
        
        if (filled == 1u) {
            alpha = 1.0 - smoothstep(radius - 0.1, radius, distance);
        } else {
            // Outline only
            let border = 0.1;
            alpha = smoothstep(radius - border - 0.1, radius - border, distance) - 
                   smoothstep(radius - 0.1, radius, distance);
        }
    }
    
    if (alpha < 0.01) {
        discard;
    }
    
    return vec4<f32>(input.color.rgb, input.color.a * alpha);
} 